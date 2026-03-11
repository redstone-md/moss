using System.Runtime.InteropServices;
using System.Text;

internal static class Native
{
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate void MossMessageCallback(IntPtr channel, IntPtr senderId, IntPtr data, uint len);

    [DllImport("moss", CallingConvention = CallingConvention.Cdecl)]
    public static extern long Moss_Init(string meshId, IntPtr psk, string config);

    [DllImport("moss", CallingConvention = CallingConvention.Cdecl)]
    public static extern int Moss_Start(long handle);

    [DllImport("moss", CallingConvention = CallingConvention.Cdecl)]
    public static extern int Moss_Stop(long handle);

    [DllImport("moss", CallingConvention = CallingConvention.Cdecl)]
    public static extern int Moss_Subscribe(long handle, string channel);

    [DllImport("moss", CallingConvention = CallingConvention.Cdecl)]
    public static extern int Moss_Publish(long handle, string channel, byte[] data, uint len);

    [DllImport("moss", CallingConvention = CallingConvention.Cdecl)]
    public static extern int Moss_SetCallback(long handle, MossMessageCallback callback);

    [DllImport("moss", CallingConvention = CallingConvention.Cdecl)]
    public static extern IntPtr Moss_GetMeshInfo(long handle);

    [DllImport("moss", CallingConvention = CallingConvention.Cdecl)]
    public static extern void Moss_Free(IntPtr ptr);
}

internal static class Program
{
    private static readonly Native.MossMessageCallback Callback = OnMessage;

    private static void OnMessage(IntPtr channel, IntPtr senderId, IntPtr data, uint len)
    {
        _ = senderId;
        var topic = Marshal.PtrToStringAnsi(channel) ?? string.Empty;
        var bytes = new byte[len];
        Marshal.Copy(data, bytes, 0, (int)len);
        Console.WriteLine($"csharp message on {topic}: {Encoding.UTF8.GetString(bytes)}");
    }

    public static void Main()
    {
        const string config = "{\"trackers\":[],\"listen_port\":41050}";
        var handle = Native.Moss_Init("demo-mesh", IntPtr.Zero, config);
        if (handle <= 0)
        {
            throw new InvalidOperationException($"Moss_Init failed: {handle}");
        }

        Native.Moss_SetCallback(handle, Callback);
        Native.Moss_Start(handle);
        Native.Moss_Subscribe(handle, "alpha");
        var payload = Encoding.UTF8.GetBytes("hello from C#");
        Native.Moss_Publish(handle, "alpha", payload, (uint)payload.Length);

        var infoPtr = Native.Moss_GetMeshInfo(handle);
        if (infoPtr != IntPtr.Zero)
        {
            Console.WriteLine(Marshal.PtrToStringAnsi(infoPtr));
            Native.Moss_Free(infoPtr);
        }

        Native.Moss_Stop(handle);
    }
}
