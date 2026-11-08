using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;

namespace DownloadServer;

public class DownloadTaskInfo
{
    public string Id { get; set; } = Guid.NewGuid().ToString("N")[..8];
    public string Url { get; set; } = "";
    public string FileName { get; set; } = "";
    public DateTime AddedAt { get; set; } = DateTime.Now;
    public string Status { get; set; } = "Очікує";
    public string? ErrorMessage { get; set; }
    public string? StoredPath { get; set; }
}

public class DownloadManager
{
    private readonly ConcurrentDictionary<string, DownloadTaskInfo> _tasks = new();
    private readonly Channel<string> _queue = Channel.CreateUnbounded<string>();
    private readonly HttpClient _httpClient = new() { Timeout = TimeSpan.FromMinutes(10) };
    private readonly string _downloadFolder;

    public DownloadManager(string downloadFolder)
    {
        _downloadFolder = downloadFolder;
        Directory.CreateDirectory(_downloadFolder);
    }

    public async Task StartWorkerAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("[DOWNLOAD] Фоновий обробник черги запущено.");
        await foreach (var id in _queue.Reader.ReadAllAsync(cancellationToken))
        {
            if (!_tasks.TryGetValue(id, out var item))
                continue;

            try
            {
                item.Status = "Завантажується";
                Console.WriteLine($"[DOWNLOAD] Почато: {item.Url}");

                using var response = await _httpClient.GetAsync(
                    item.Url, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
                response.EnsureSuccessStatusCode();

                var dispositionName = response.Content.Headers.ContentDisposition?.FileNameStar
                    ?? response.Content.Headers.ContentDisposition?.FileName;
                var fileName = !string.IsNullOrWhiteSpace(dispositionName)
                    ? dispositionName.Trim('"')
                    : Path.GetFileName(new Uri(item.Url).AbsolutePath);

                if (string.IsNullOrWhiteSpace(fileName))
                    fileName = "download.bin";

                fileName = SanitizeFileName(fileName);
                var storedName = $"{item.Id}_{fileName}";
                var fullPath = Path.Combine(_downloadFolder, storedName);

                await using (var input = await response.Content.ReadAsStreamAsync(cancellationToken))
                await using (var output = new FileStream(fullPath, FileMode.Create, FileAccess.Write, FileShare.None))
                {
                    await input.CopyToAsync(output, cancellationToken);
                }

                item.FileName = fileName;
                item.StoredPath = fullPath;
                item.Status = "Завантажено";
                Console.WriteLine($"[DOWNLOAD] Готово: {fileName}");
            }
            catch (Exception ex)
            {
                item.Status = "Помилка";
                item.ErrorMessage = ex.Message;
                Console.WriteLine($"[DOWNLOAD] Помилка для {id}: {ex.Message}");
            }
        }
    }

    public DownloadTaskInfo Add(string url)
    {
        if (!Uri.TryCreate(url, UriKind.Absolute, out var uri) ||
            (uri.Scheme != Uri.UriSchemeHttp && uri.Scheme != Uri.UriSchemeHttps))
            throw new ArgumentException("Потрібно вказати коректне HTTP або HTTPS посилання.");

        var item = new DownloadTaskInfo { Url = uri.ToString() };
        _tasks[item.Id] = item;
        if (!_queue.Writer.TryWrite(item.Id))
        {
            _tasks.TryRemove(item.Id, out _);
            throw new InvalidOperationException("Не вдалося додати завдання до черги.");
        }

        return item;
    }

    public List<DownloadTaskInfo> GetAll() =>
        _tasks.Values.OrderByDescending(x => x.AddedAt).ToList();

    public bool TryGet(string id, out DownloadTaskInfo? item) =>
        _tasks.TryGetValue(id, out item);

    private static string SanitizeFileName(string name)
    {
        foreach (var c in Path.GetInvalidFileNameChars())
            name = name.Replace(c, '_');
        name = name.Replace("/", "_").Replace("\\", "_");
        return string.IsNullOrWhiteSpace(name) ? "download.bin" : name;
    }
}

public class Request
{
    public string Command { get; set; } = "";
    public string? Url { get; set; }
    public string? Id { get; set; }
}

public class Response
{
    public bool Success { get; set; }
    public string Message { get; set; } = "";
    public object? Data { get; set; }
    public long? FileSize { get; set; }
    public string? FileName { get; set; }
}

internal static class Program
{
    private const int Port = 5000;
    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };

    private static async Task Main()
    {
        using var cts = new CancellationTokenSource();
        var manager = new DownloadManager(Path.Combine(AppContext.BaseDirectory, "downloads"));
        _ = Task.Run(() => manager.StartWorkerAsync(cts.Token));

        var listener = new TcpListener(IPAddress.Any, Port);
        listener.Start();

        Console.WriteLine("======================================");
        Console.WriteLine("     NETWORK DOWNLOAD MANAGER SERVER");
        Console.WriteLine("======================================");
        Console.WriteLine($"Сервер запущено. Порт: {Port}");
        Console.WriteLine("Очікування клієнтів...");

        try
        {
            while (true)
            {
                var tcpClient = await listener.AcceptTcpClientAsync();
                _ = Task.Run(() => HandleClientAsync(tcpClient, manager));
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Помилка сервера: {ex.Message}");
        }
        finally
        {
            listener.Stop();
            cts.Cancel();
        }
    }

    private static async Task HandleClientAsync(TcpClient client, DownloadManager manager)
    {
        using (client)
        {
            try
            {
                client.NoDelay = true;
                await using var stream = client.GetStream();
                var line = await ReadLineAsync(stream);
                if (line is null)
                    return;

                var request = JsonSerializer.Deserialize<Request>(line, JsonOptions);
                if (request is null)
                {
                    await SendJsonAsync(stream, new Response { Success = false, Message = "Некоректний запит." });
                    return;
                }

                switch (request.Command.ToLowerInvariant())
                {
                    case "add":
                        try
                        {
                            var added = manager.Add(request.Url ?? "");
                            await SendJsonAsync(stream, new Response
                            {
                                Success = true,
                                Message = "Завдання додано до черги.",
                                Data = added
                            });
                        }
                        catch (Exception ex)
                        {
                            await SendJsonAsync(stream, new Response { Success = false, Message = ex.Message });
                        }
                        break;

                    case "list":
                        await SendJsonAsync(stream, new Response
                        {
                            Success = true,
                            Message = "Список завантажень.",
                            Data = manager.GetAll()
                        });
                        break;

                    case "download":
                        if (string.IsNullOrWhiteSpace(request.Id) ||
                            !manager.TryGet(request.Id, out var item) || item is null)
                        {
                            await SendJsonAsync(stream, new Response { Success = false, Message = "Завантаження не знайдено." });
                            break;
                        }

                        if (item.Status != "Завантажено" || string.IsNullOrWhiteSpace(item.StoredPath) ||
                            !File.Exists(item.StoredPath))
                        {
                            await SendJsonAsync(stream, new Response { Success = false, Message = "Файл ще не готовий або недоступний." });
                            break;
                        }

                        var fileInfo = new FileInfo(item.StoredPath);
                        await SendJsonAsync(stream, new Response
                        {
                            Success = true,
                            Message = "Передавання файлу розпочато.",
                            FileName = item.FileName,
                            FileSize = fileInfo.Length
                        });

                        await using (var file = new FileStream(item.StoredPath, FileMode.Open, FileAccess.Read, FileShare.Read))
                            await file.CopyToAsync(stream);
                        break;

                    default:
                        await SendJsonAsync(stream, new Response { Success = false, Message = "Невідома команда." });
                        break;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Клієнт від'єднався або сталася помилка: {ex.Message}");
            }
        }
    }

    private static async Task SendJsonAsync(NetworkStream stream, Response response)
    {
        var json = JsonSerializer.Serialize(response);
        var bytes = Encoding.UTF8.GetBytes(json + "\n");
        await stream.WriteAsync(bytes);
        await stream.FlushAsync();
    }

    private static async Task<string?> ReadLineAsync(NetworkStream stream)
    {
        using var memory = new MemoryStream();
        var one = new byte[1];
        while (true)
        {
            var count = await stream.ReadAsync(one);
            if (count == 0)
                return memory.Length == 0 ? null : Encoding.UTF8.GetString(memory.ToArray());
            if (one[0] == (byte)'\n')
                return Encoding.UTF8.GetString(memory.ToArray()).TrimEnd('\r');
            memory.WriteByte(one[0]);
            if (memory.Length > 1024 * 1024)
                throw new InvalidDataException("Занадто великий запит.");
        }
    }
}
