// Copyright (c) Alexandre Mutel. All rights reserved.
// Licensed under the BSD-Clause 2 license.
// See license.txt file in the project root for full license information.

using System.Collections;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO.MemoryMappedFiles;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Text;
using Microsoft.Win32.SafeHandles;

namespace Fast1BRC;

/// <summary>
/// C# version for The One Billion Row Challenge
/// https://github.com/gunnarmorling/1brc
/// Author: xoofx
/// </summary>
internal static unsafe class Program
{
    private const int ReadBufferSize = 256 << 10;
    private const int ExtraBufferSize = 256;

    static void Main(string[] args)
    {
        try
        {
            Process.GetCurrentProcess().PriorityClass = ProcessPriorityClass.RealTime;
        }
        catch
        {
            // Ignore
        }

        var filePath = args.FirstOrDefault(x => !x.StartsWith("--")) ?? Path.Combine(Environment.CurrentDirectory, "measurements-1_000_000-sample.txt");
        if (!File.Exists(filePath)) throw new FileNotFoundException(filePath);

        RunOptions options;
        options.Pgo = args.Contains("--pgo");
        options.NoThreads = args.Contains("--nothreads");
        options.Verbose = args.Contains("--verbose") || args.Contains("-v");
        options.Timing = args.Contains("--time") || args.Contains("-t");
        options.Mmap = !args.Contains("--nommap") && (args.Contains("--mmap") || OperatingSystem.IsMacOS());
        
        Console.OutputEncoding = Encoding.UTF8;

        // Use RandomAccess on Windows and Linux, mmap on macOS
        var count = options.Pgo ? 10 : 1;
        for (int i = 0; i < count; i++)
        {
            if (i > 0) Console.WriteLine();
            var clock = Stopwatch.StartNew();
#if DEVEL
            Console.Write(RunDevel(filePath, options));
#else
            Console.Write(Run(filePath, options));
#endif
            clock.Stop();
            if (options.Timing)
            {
                Console.WriteLine();
                Console.WriteLine($"Elapsed in {clock.Elapsed.TotalMilliseconds} ms");
            }
        }
    }

    /// <summary>
    /// Main used during development
    /// </summary>
    private static string RunDevel(string filePath, RunOptions options)
    {
        if (options.Pgo)
        {
            // Run the program with PGO
            RunGeneric<MemoryMapped1RBCImpl>(filePath, options);
            RunGeneric<RandomAccess1BRCImpl>(filePath, options);
        }

        if (options.Mmap)
        {
            return RunGeneric<MemoryMapped1RBCImpl>(filePath, options);
        }
        else
        {
            return RunGeneric<RandomAccess1BRCImpl>(filePath, options);
        }
    }

    /// <summary>
    /// Main used for benchmarks (will compile only a single version of it)
    /// </summary>
    private static string Run(string filePath, RunOptions options)
    {
        if (OperatingSystem.IsMacOS())
        {
            return RunGeneric<MemoryMapped1RBCImpl>(filePath, options);
        }
        else
        {
            return RunGeneric<RandomAccess1BRCImpl>(filePath, options);
        }
    }

    private readonly struct MemoryMapped1RBCImpl : I1BRCImpl
    {
        public static DictionaryGroup ProcessChunk(string file, long startOffset, long endOffsetNotInclusive)
            => ProcessChunkMemoryMapped(file, startOffset, endOffsetNotInclusive);
    }

    private readonly struct RandomAccess1BRCImpl : I1BRCImpl
    {
        public static DictionaryGroup ProcessChunk(string file, long startOffset, long endOffsetNotInclusive)
            => ProcessChunkRandomAccess(file, startOffset, endOffsetNotInclusive);
    }
    
    private interface I1BRCImpl
    {
        static abstract DictionaryGroup ProcessChunk(string file, long startOffset, long endOffsetNotInclusive);
    }
    
    private struct RunOptions
    {
        public bool Pgo;

        public bool NoThreads;

        public bool Verbose;

        public bool Timing;

        public bool Mmap;
    }
    
    private static string RunGeneric<TImpl>(string filePath, RunOptions options) where TImpl: I1BRCImpl
    {
        using var fileHandle = File.OpenHandle(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, FileOptions.None);
        var fileLength = RandomAccess.GetLength(fileHandle);

        // --------------------------------------------------------------------
        // Split the file by chunks and process them in parallel
        // --------------------------------------------------------------------
        var clock = Stopwatch.StartNew();
        Span<byte> localBuffer = stackalloc byte[256];
        var minCount = (int)Math.Max(fileLength / int.MaxValue, 1);
        // As we are running one chunk on the main thread, we need to keep one core free
        // But on machines we not enough core, we want to give the JIT a chance to optimize the code, so we give another core available
        var taskCount = Math.Max(minCount, Environment.ProcessorCount - (Environment.ProcessorCount < 16 ? 2 : 1));
        if (options.Verbose)
        {
            Console.WriteLine($"Using {taskCount} tasks");
        }
        var threads = new List<Thread>(taskCount);
        var results = new DictionaryGroup[taskCount];
        var chunkSize = fileLength / taskCount;
        long startOffset = 0;
        for (int i = 0; i < taskCount; i++)
        {
            long endOffset;
            if (i + 1 < taskCount)
            {
                endOffset = (i + 1) * chunkSize;

                RandomAccess.Read(fileHandle, localBuffer, endOffset);
                var indexOfEndOfLine = localBuffer.IndexOf((byte)'\n');
                Debug.Assert(indexOfEndOfLine >= 0);
                endOffset += indexOfEndOfLine + 1;
            }
            else
            {
                endOffset = fileLength;
            }

            long localStartOffset = startOffset;
            if (options.NoThreads || i + 1 == taskCount)
            {
                // For the last chunk, we always process on current thread and we never use mmmap
                results[i] = ProcessChunkRandomAccess(fileHandle, localStartOffset, endOffset);
            }
            else
            {
                var localIndex = i;
                var thread = new Thread(() =>
                {
                    results[localIndex] = TImpl.ProcessChunk(filePath, localStartOffset, endOffset);
                })
                {
                    Priority = ThreadPriority.Highest
                };
                thread.Start();
                threads.Add(thread);
            }

            startOffset = endOffset;
        }

        // --------------------------------------------------------------------
        // Aggregate the results
        // --------------------------------------------------------------------
        var result = new Dictionary<string, EntryItem>(11000);
        if (results.Length > 0)
        {
            var dict = results[^1];
            dict.AggregateTo(result);
            dict.Dispose();

            for (int i = 0; i < results.Length - 1; i++)
            {
                if (i < threads.Count)
                {
                    threads[i].Join();
                }

                dict = results[i];
                dict.AggregateTo(result);
                if (options.Verbose)
                {
                    Console.WriteLine(dict.GetStatistics());
                }
                dict.Dispose();
            }
        }

        // --------------------------------------------------------------------
        // Format the results
        // --------------------------------------------------------------------
        var builder = new StringBuilder(result.Count * 100);
        builder.Append('{');
        bool isFirst = true;
        foreach (var pair in result.OrderBy(x => (string)x.Key!, StringComparer.Ordinal))
        {
            if (!isFirst) builder.Append(", ");
            builder.Append($"{pair.Key}={(pair.Value.MinTemp / 10.0):0.0}/{pair.Value.SumTemp / (10.0 * pair.Value.Count):0.0}/{(pair.Value.MaxTemp / 10.0):0.0}");
            isFirst = false;
        }
        builder.Append('}');

        return builder.ToString();
    }
    

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private static DictionaryGroup ProcessChunkRandomAccess(string file, long startOffset, long endOffsetNotInclusive)
    {
        using var localFileHandle = File.OpenHandle(file, FileMode.Open, FileAccess.Read, FileShare.Read, FileOptions.None);
        return ProcessChunkRandomAccess(localFileHandle, startOffset, endOffsetNotInclusive);
    }

    /// <summary>
    /// Process a chunk of the file
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private static DictionaryGroup ProcessChunkRandomAccess(SafeFileHandle localFileHandle, long startOffset, long endOffsetNotInclusive)
    {
        // Reopen the file to improve concurrency, as it seems - at least on Windows - that it is more efficient to have multiple handles
        var managedBuffer = GC.AllocateUninitializedArray<byte>(ReadBufferSize + ExtraBufferSize * 2 + Vector256<byte>.Count, true);
        var handle = GCHandle.Alloc(managedBuffer, GCHandleType.Pinned);
        var spanEnd = new Span<byte>(managedBuffer, ReadBufferSize + ExtraBufferSize * 2, Vector256<byte>.Count);
        spanEnd.Clear();
        //var originalBuffer = (byte*)NativeMemory.AlignedAlloc((nuint) (ReadBufferSize + ExtraBufferSize), 4096);  // (byte*)handle.AddrOfPinnedObject() + ExtraBufferSize;
        var buffer = (byte*)handle.AddrOfPinnedObject() + ExtraBufferSize;

        var entries = new DictionaryGroup();
        long fileOffset = startOffset;
        nint bufferOffset = 0; // in case we have a line that is not fully read
        while (fileOffset < endOffsetNotInclusive)
        {
            // Read the next buffer
            var maxLengthToRead = Math.Min(ReadBufferSize + ExtraBufferSize, endOffsetNotInclusive - fileOffset);
            var bufferLength = RandomAccess.Read(localFileHandle, new Span<byte>(buffer, (int)maxLengthToRead), fileOffset);
            fileOffset += bufferLength;

            // Process the buffer
            bool isLastBuffer = fileOffset == endOffsetNotInclusive;
            if (isLastBuffer)
            {
                bufferLength += (int)bufferOffset;
                ProcessBuffer(entries, buffer - bufferOffset, bufferLength);
            }
            else
            {
                var slice = new Span<byte>(buffer + ReadBufferSize, ExtraBufferSize);
                bufferLength = ReadBufferSize + slice.LastIndexOf((byte)'\n') + 1 + (int)bufferOffset;
                ProcessBuffer(entries, buffer - bufferOffset, bufferLength);

                bufferOffset = ReadBufferSize + ExtraBufferSize + bufferOffset - bufferLength;
                if (bufferOffset > 0)
                {
                    // Copy the remaining bytes to the beginning of the buffer (A line that was not fully read)
                    Unsafe.CopyBlockUnaligned(buffer - bufferOffset, buffer + ReadBufferSize + ExtraBufferSize - bufferOffset, (uint)bufferOffset);
                }
            }
        }

        //NativeMemory.AlignedFree((void*)originalBuffer);
        handle.Free();

        return entries;
    }

    /// <summary>
    /// Process a buffer
    /// </summary>
    /// <returns>An index to the remaining buffer that hasn't been processed because the line was not complete; otherwise -1</returns>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private static DictionaryGroup ProcessChunkMemoryMapped(string filePath, long startOffset, long endOffset)
    {
        var entries = new DictionaryGroup();

        using var fileHandle = File.OpenHandle(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, FileOptions.RandomAccess);
        using var mappedFile = MemoryMappedFile.CreateFromFile(fileHandle, null, 0, MemoryMappedFileAccess.Read, HandleInheritability.None, true);

        var bufferLength = endOffset - startOffset;
        using var viewAccessor = mappedFile.CreateViewAccessor(startOffset, bufferLength, MemoryMappedFileAccess.Read);
        var handle = viewAccessor.SafeMemoryMappedViewHandle;
        byte* buffer = null;
        handle.AcquirePointer(ref buffer);
        ProcessBuffer(entries, buffer + viewAccessor.PointerOffset, bufferLength);

        handle.ReleasePointer();

        return entries;
    }


    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ProcessBuffer(DictionaryGroup entries, byte* buffer, nint bufferLength)
    {
        //if (Vector256.IsHardwareAccelerated)
        {
            ProcessBuffer256(entries, buffer, bufferLength);
        }
        //else if (Vector128.IsHardwareAccelerated)
        //{
        //    ProcessBuffer128(entries, buffer, bufferLength);
        //}
        //else
        //{
        //    ProcessBufferSimple(entries, buffer, bufferLength);
        //}
    }

    /// <summary>
    /// Process a buffer
    /// </summary>
    /// <returns>An index to the remaining buffer that hasn't been processed because the line was not complete; otherwise -1</returns>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private static void ProcessBuffer256(DictionaryGroup entries, byte* buffer, nint bufferLength)
    {
        nint index = 0;
        while (index < bufferLength)
        {
            nint startLineIndex = index;

            var destName = entries.NamePointer;
            var last = Vector256<byte>.Zero;
            int nameLength;
            while (true)
            {
                var mask = Vector256.Create((byte)';');
                var v = Vector256.Load(buffer + index);
                var eq = Vector256.Equals(v, mask);
                if (eq == Vector256<byte>.Zero)
                {
                    v.Store(destName);
                    last = v;
                    destName += Vector256<byte>.Count;
                    index += Vector256<byte>.Count;
                }
                else
                {
                    // The string is shorter than 16 bytes, we fetch the index of `;`
                    var offset = BitOperations.TrailingZeroCount(eq.ExtractMostSignificantBits());
                    // We calculate the corresponding mask
                    var val = Vector256.Create((byte)offset);
                    var indices = Vector256.Create((byte)0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31);
                    v = (Vector256.GreaterThan(val, indices) & v);
                    index += offset;
                    nameLength = (int)(index - startLineIndex);
                    if (nameLength < 32)
                    {
                        last = v;
                    }
                    else if (nameLength > 32)
                    {
                        v.Store(destName);
                    }
                    break;
                }
            }

            index++;

            // --------------------------------------------------------------------
            // Process the temperature
            // --------------------------------------------------------------------
            int sign = 1;
            int temp = 0;
            while (true)
            {
                var c = *(buffer + index++);
                if (c == (byte)'-')
                {
                    sign = -1;
                }
                else if (c == (byte)'\n')
                {
                    temp *= sign;
                    break;
                }
                else if ((char)c != '.')
                {
                    temp = temp * 10 + (c - '0');
                }
            }

            // --------------------------------------------------------------------
            // Add the entry
            // --------------------------------------------------------------------
            ref var entry = ref entries.GetOrAdd(last, nameLength);
            if (entry.Count == 0)
            {
                entry.MinTemp = int.MaxValue;
            }
            entry.Count++;
            entry.SumTemp += temp;
            entry.MinTemp = Math.Min(entry.MinTemp, temp);
            entry.MaxTemp = Math.Max(entry.MaxTemp, temp);
        }
    }

    /// <summary>
    /// Process a buffer
    /// </summary>
    /// <returns>An index to the remaining buffer that hasn't been processed because the line was not complete; otherwise -1</returns>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private static void ProcessBuffer128(DictionaryGroup entries, byte* buffer, nint bufferLength)
    {
        nint index = 0;
        while (index < bufferLength)
        {
            nint startLineIndex = index;

            var destName = entries.NamePointer;
            var last = Vector128<byte>.Zero;
            int nameLength;
            while (true)
            {
                var mask = Vector128.Create((byte)';');
                var v = Vector128.Load(buffer + index);
                var eq = Vector128.Equals(v, mask);
                if (eq == Vector128<byte>.Zero)
                {
                    v.Store(destName);
                    last = v;
                    destName += Vector128<byte>.Count;
                    index += Vector128<byte>.Count;
                }
                else
                {
                    // The string is shorter than 16 bytes, we fetch the index of `;`
                    var offset = BitOperations.TrailingZeroCount(eq.ExtractMostSignificantBits());
                    // We calculate the corresponding mask
                    var val = Vector128.Create((byte)offset);
                    var indices = Vector128.Create((byte)0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
                    v = (Vector128.GreaterThan(val, indices) & v);
                    index += offset;
                    nameLength = (int)(index - startLineIndex);
                    if (nameLength > 16)
                    {
                        v.Store(destName);
                    }
                    else if (nameLength < 16)
                    {
                        last = v;
                    }
                    break;
                }
            }

            index++; // Skip comma

            // --------------------------------------------------------------------
            // Process the temperature
            // --------------------------------------------------------------------
            int sign = 1;
            int temp = 0;
            while (true)
            {
                var c = *(buffer + index++);
                if (c == (byte)'-')
                {
                    sign = -1;
                }
                else if (c == (byte)'\n')
                {
                    temp *= sign;
                    break;
                }
                else if ((char)c != '.')
                {
                    temp = temp * 10 + (c - '0');
                }
            }

            // --------------------------------------------------------------------
            // Add the entry
            // --------------------------------------------------------------------
            ref var entry = ref entries.GetOrAdd(last, nameLength);
            if (entry.Count == 0)
            {
                entry.MinTemp = int.MaxValue;
            }
            entry.Count++;
            entry.SumTemp += temp;
            entry.MinTemp = Math.Min(entry.MinTemp, temp);
            entry.MaxTemp = Math.Max(entry.MaxTemp, temp);
        }
    }

    /// <summary>
    /// Process a buffer
    /// </summary>
    /// <returns>An index to the remaining buffer that hasn't been processed because the line was not complete; otherwise -1</returns>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private static void ProcessBufferSimple(DictionaryGroup entries, byte* buffer, nint bufferLength)
    {
        nint index = 0;
        while (index < bufferLength)
        {
            nint startLineIndex = index;

            var destName = entries.NamePointer;
            while (true)
            {
                var c = *(buffer + index);
                if (c == (byte)';')
                {
                    break;
                }
                *destName++ = c;
                index++;
            }

            int nameLength = (int)(index++ - startLineIndex);

            // --------------------------------------------------------------------
            // Process the temperature
            // --------------------------------------------------------------------
            int sign = 1;
            int temp = 0;
            while (true)
            {
                var c = *(buffer + index++);
                if (c == (byte)'-')
                {
                    sign = -1;
                }
                else if (c == (byte)'\n')
                {
                    temp *= sign;
                    break;
                }
                else if ((char)c != '.')
                {
                    temp = temp * 10 + (c - '0');
                }
            }

            // --------------------------------------------------------------------
            // Add the entry
            // --------------------------------------------------------------------
            ref var entry = ref entries.GetOrAdd(nameLength);
            if (entry.Count == 0)
            {
                entry.MinTemp = int.MaxValue;
            }
            entry.Count++;
            entry.SumTemp += temp;
            entry.MinTemp = Math.Min(entry.MinTemp, temp);
            entry.MaxTemp = Math.Max(entry.MaxTemp, temp);
        }
    }


    private class DictionaryGroup
    {
        private FastDictionary<KeyName16, EntryItem> _entries16 = new(6000);
        private FastDictionary<KeyName32, EntryItem> _entries32 = new(6000);
        private FastDictionary<KeyName128, EntryItem> _entriesAny = new(2000);
        private readonly KeyName128* _name128;

        public DictionaryGroup()
        {
            _name128 = (KeyName128*)NativeMemory.AlignedAlloc((nuint)sizeof(KeyName128) , CacheLineSize);
        }

        public void Dispose()
        {
            NativeMemory.AlignedFree(_name128);
            _entries16.Dispose();
            _entries32.Dispose();
            _entriesAny.Dispose();
        }

        public byte* NamePointer => (byte*)_name128;

        public void AggregateTo(Dictionary<string, EntryItem> result)
        {
            foreach (var item in GetValues())
            {
                string? name = item.Item1!;
                ref var existingValue = ref CollectionsMarshal.GetValueRefOrAddDefault(result, name, out _);
                existingValue.AggregateFrom(in item.Item2);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        public IEnumerable<(string, EntryItem)> GetValues()
        {
            foreach (var entry in _entries16)
            {
                yield return (entry.Key.ToString(), entry.Value);
            }

            foreach (var entry in _entries32)
            {
                yield return (entry.Key.ToString(), entry.Value);
            }

            foreach (var entry in _entriesAny)
            {
                yield return (entry.Key.ToString(), entry.Value);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref EntryItem GetOrAdd(Vector256<byte> value, int length)
        {
            if (length <= 32)
            {
                return ref _entries32.GetValueRefOrAddDefault(value); ;
            }

            ClearName(length);
            return ref _entriesAny.GetValueRefOrAddDefault(in *_name128);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref EntryItem GetOrAdd(Vector128<byte> value, int length)
        {
            if (length <= 16)
            {
                return ref _entries16.GetValueRefOrAddDefault(value);
            }

            var names = NamePointer;
            if (length <= 32)
            {
                return ref _entries32.GetValueRefOrAddDefault(in *(KeyName32*)names);
            }

            ClearName(length);
            return ref _entriesAny.GetValueRefOrAddDefault(in *_name128);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref EntryItem GetOrAdd(int length)
        {
            var names = NamePointer;
            if (length <= 16)
            {
                return ref _entries16.GetValueRefOrAddDefault(in *(KeyName16*)names);
            }

            if (length <= 32)
            {
                return ref _entries32.GetValueRefOrAddDefault(in *(KeyName32*)names);
            }

            ClearName(length);
            return ref _entriesAny.GetValueRefOrAddDefault(in *_name128);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ClearName(int length)
        {
            Unsafe.InitBlockUnaligned(NamePointer + length, 0, (uint)(sizeof(KeyName128) - length));
        }

        public string GetStatistics()
        {
            return $"Dictionary 16: {_entries16.Count} 32: {_entries32.Count} - Any: {_entriesAny.Count}";
        }
    }

    private readonly struct KeyName16 : IEquatable<KeyName16>
    {
        //private readonly Vector128<byte> _name;
        private readonly long _name1;
#pragma warning disable CS0649 // Field is never assigned to, and will always have its default value
        private readonly long _name2;
#pragma warning restore CS0649 // Field is never assigned to, and will always have its default value

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(KeyName16 other)
        {
            if (Vector128.IsHardwareAccelerated)
            {
                return ToVector128().Equals(other.ToVector128());
            }
            return _name1 == other._name1 && _name2 == other._name2;
        }

        public override bool Equals(object? obj) => obj is KeyName16 other && Equals(other);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        //public override int GetHashCode() => (Unsafe.As<KeyName16, ulong>(ref Unsafe.AsRef(in this)) * 397).GetHashCode();
        public override int GetHashCode() => ((_name1 * 397) ^ _name2).GetHashCode();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetHashCode(Vector128<byte> key) => ((key.AsInt64().GetElement(0) * 397) ^ key.AsInt64().GetElement(1)).GetHashCode();
        
        public override string ToString()
        {
            fixed (void* name = &_name1)
            {
                var span = new Span<byte>(name, 16);
                var indexOf0 = span.IndexOf((byte) 0);
                if (indexOf0 >= 0)
                {
                    span = span.Slice(0, indexOf0);
                }

                return Encoding.UTF8.GetString(span);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Vector128<byte> ToVector128() => Unsafe.BitCast<KeyName16, Vector128<byte>>(this);
    }

    private readonly struct KeyName32 : IEquatable<KeyName32>
    {
        private readonly long _name1;
#pragma warning disable CS0649 // Field is never assigned to, and will always have its default value
        private readonly long _name2;
        private readonly long _name3;
        private readonly long _name4;
#pragma warning restore CS0649 // Field is never assigned to, and will always have its default value

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(KeyName32 other)
        {
            if (Vector256.IsHardwareAccelerated)
            {
                return ToVector256().Equals(other.ToVector256());
            }

            if (Vector128.IsHardwareAccelerated)
            {
                return ToVector128Low() == other.ToVector128Low() && ToVector128High() == other.ToVector128High();
            }

            return _name1 == other._name1 && _name2 == other._name2 && _name3 == other._name3 && _name4 == other._name4;
        }

        public override bool Equals(object? obj) => obj is KeyName32 other && Equals(other);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override int GetHashCode() => ((_name1 * 397) ^ _name2).GetHashCode();

        public override string ToString()
        {
            fixed (void* name = &_name1)
            {
                var span = new Span<byte>(name, 32);
                var indexOf0 = span.IndexOf((byte)0);
                if (indexOf0 >= 0)
                {
                    span = span.Slice(0, indexOf0);
                }

                return Encoding.UTF8.GetString(span);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetHashCode(Vector256<byte> key) => ((key.AsInt64().GetElement(0) * 397) ^ key.AsInt64().GetElement(1)).GetHashCode();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Vector256<byte> ToVector256() => Unsafe.BitCast<KeyName32, Vector256<byte>>(this);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Vector128<byte> ToVector128Low() => Unsafe.As<KeyName32, Vector128<byte>>(ref Unsafe.AsRef(in this));

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Vector128<byte> ToVector128High() => Unsafe.Add(ref Unsafe.As<KeyName32, Vector128<byte>>(ref Unsafe.AsRef(in this)), 1);
    }

    private readonly struct KeyName128 : IEquatable<KeyName128>
    {
        private readonly Vector256<byte> _name1;
        private readonly Vector256<byte> _name2;
        private readonly Vector256<byte> _name3;
        private readonly Vector256<byte> _name4;

        public KeyName128(byte* name, int length)
        {
            _name1 = _name2 = _name3 = _name4 = Vector256<byte>.Zero;
            Unsafe.CopyBlockUnaligned(ref Unsafe.As<Vector256<byte>, byte>(ref _name1), ref *name, (uint)length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(KeyName128 other)
        {
            return _name1 == other._name1 && _name2 == other._name2 && _name3 == other._name3 && _name4 == other._name4;
        }

        public override bool Equals(object? obj) => obj is KeyName128 other && Equals(other);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override int GetHashCode() => ((_name1.AsInt64().GetElement(0) * 397) ^ _name1.AsInt64().GetElement(1)).GetHashCode();

        public override string ToString()
        {
            fixed (void* name = &_name1)
            {
                var span = new Span<byte>(name, 128);
                var indexOf0 = span.IndexOf((byte)0);
                if (indexOf0 >= 0)
                {
                    span = span.Slice(0, indexOf0);
                }

                return Encoding.UTF8.GetString(span);
            }
        }
    }

    /// <summary>
    /// Structure to hold the entry data per city
    /// </summary>
    private struct EntryItem
    {
        public long SumTemp;
        public long Count;
        public int MinTemp;
        public int MaxTemp;

        public static int GetSizeOf() => sizeof(EntryItem);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void AggregateFrom(in EntryItem item)
        {
            SumTemp += item.SumTemp;
            Count += item.Count;
            MinTemp = Math.Min(item.MinTemp, MinTemp);
            MaxTemp = Math.Max(item.MaxTemp, MaxTemp);
        }
    }

    private const int CacheLineSize = 64;
    
    [DebuggerTypeProxy(typeof(FastDictionary<,>.IDictionaryDebugView))]
    [DebuggerDisplay("Count = {Count}")]
    public struct FastDictionary<TKey, TValue> where TKey : unmanaged, IEquatable<TKey> where TValue: unmanaged
    {
        private Entry** _buckets;
        private Entry* _entries;
        private int _capacity;
        private int _count;

        public FastDictionary() : this(0) { }

        public FastDictionary(int capacity)
        {
            Initialize(Math.Max(capacity, 4));
            Debug.Assert(sizeof(Entry) == CacheLineSize);
        }

        public int Count => _count;

        public void Dispose()
        {
            NativeMemory.AlignedFree(_buckets);
            NativeMemory.AlignedFree(_entries);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref TValue GetValueRefOrAddDefault(Vector256<byte> key)
        {
            uint hashCode = (uint)KeyName32.GetHashCode(key);

#if DEBUG
            Debug.Assert((uint)Unsafe.BitCast<Vector256<byte>, TKey>(key).GetHashCode() == hashCode);
#endif

            ref Entry* bucket = ref GetBucket(hashCode);
            for (Entry* entry = bucket; entry != null; entry = entry->next)
            {
                if (Vector256.LoadAligned((byte*)&entry->key) == key)
                {
                    return ref entry->value;
                }
#if DEBUG
                if (entry->key.GetHashCode() == hashCode)
                {
                    Console.WriteLine($"Collision {hashCode} {entry->key} <=> {key}");
                }
#endif
            }

            int count = _count;
            if (count == _capacity)
            {
                Resize();
#if DEBUG
                Console.WriteLine($"Resize from {count} to {_capacity}");
#endif
                bucket = ref GetBucket(hashCode);
            }
            int index = count;
            _count = count + 1;

            var entries = _entries;
            var newEntry = entries + index;
            newEntry->key = Unsafe.BitCast<Vector256<byte>, TKey>(key);
            newEntry->value = default!;
            newEntry->next = bucket;
            bucket = newEntry;

            return ref newEntry->value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref TValue GetValueRefOrAddDefault(Vector128<byte> key)
        {
            uint hashCode = (uint)KeyName16.GetHashCode(key);
            ref Entry* bucket = ref GetBucket(hashCode);
            for (Entry* entry = bucket; entry != null; entry = entry->next)
            {
                if (Vector128.LoadAligned((byte*)&entry->key) == key)
                {
                    return ref entry->value;
                }
#if DEBUG
                if (entry->key.GetHashCode() == hashCode)
                {
                    Console.WriteLine($"Collision {hashCode} {entry->key} <=> {key}");
                }
#endif
            }

            int count = _count;
            if (count == _capacity)
            {
                Resize();
#if DEBUG
                Console.WriteLine($"Resize from {count} to {_capacity}");
#endif
                bucket = ref GetBucket(hashCode);
            }
            int index = count;
            _count = count + 1;

            var entries = _entries;
            var newEntry = entries + index;
            newEntry->key = Unsafe.BitCast<Vector128<byte>, TKey>(key);
            newEntry->value = default!;
            newEntry->next = bucket;
            bucket = newEntry;

            return ref newEntry->value;
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref TValue GetValueRefOrAddDefault(in TKey key)
        {
            uint hashCode = (uint)(key!.GetHashCode());
            ref Entry* bucket = ref GetBucket(hashCode);
            for (Entry* entry = bucket; entry != null; entry = entry->next)
            {
                if (entry->key.Equals(key))
                {
                    return ref entry->value;
                }
#if DEBUG
                if (entry->key.GetHashCode() == hashCode)
                {
                    Console.WriteLine($"Collision {hashCode} {entry->key} <=> {key}");
                }
#endif
            }

            int count = _count;
            if (count == _capacity)
            {
                Resize();
#if DEBUG
                Console.WriteLine($"Resize from {count} to {_capacity}");
#endif
                bucket = ref GetBucket(hashCode);
            }
            int index = count;
            _count = count + 1;

            var entries = _entries;
            var newEntry = entries + index;
            newEntry->key = key;
            newEntry->value = default!;
            newEntry->next = bucket;
            bucket = newEntry;

            return ref newEntry->value;
        }

        public Enumerator GetEnumerator() => new Enumerator(this, Enumerator.KeyValuePair);

        private int Initialize(int capacity)
        {
            int size = HashHelpers.GetPrime(capacity);
            Entry** buckets = (Entry**) NativeMemory.AlignedAlloc((nuint) (size * sizeof(Entry*)), CacheLineSize);
            new Span<nint>(buckets, size).Clear();
            Entry* entries = (Entry*) NativeMemory.AlignedAlloc((nuint) (size * sizeof(Entry)), CacheLineSize);

            // Assign member variables after both arrays allocated to guard against corruption from OOM if second fails
            _buckets = buckets;
            _entries = entries;
            _capacity = size;

            return size;
        }

        private void Resize() => Resize(HashHelpers.ExpandPrime(_count));

        private void Resize(int newSize)
        {
            // Value types never rehash
            Debug.Assert(_entries != null, "_entries should be non-null");
            Debug.Assert(newSize >= _capacity);

            Entry* entries = (Entry*)NativeMemory.AlignedAlloc((nuint)(newSize * sizeof(Entry)), CacheLineSize);
            var newSpan = new Span<Entry>(entries, newSize);

            int count = _count;
            new Span<Entry>(_entries, count).CopyTo(newSpan);
            NativeMemory.AlignedFree(_entries);

            // Assign member variables after both arrays allocated to guard against corruption from OOM if second fails
            NativeMemory.AlignedFree(_buckets);
            _buckets = (Entry**) NativeMemory.AlignedAlloc((nuint) (newSize * sizeof(Entry*)), CacheLineSize);
            new Span<nint>(_buckets, newSize).Clear();
            _capacity = newSize;

            for (int i = 0; i < count; i++)
            {
                ref Entry* bucket = ref GetBucket((uint)entries[i].key.GetHashCode());
                entries[i].next = bucket;
                bucket = entries + i;
            }

            _entries = entries;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ref Entry* GetBucket(uint hashCode)
        {
            Entry** buckets = _buckets!;
            return ref buckets[(uint)hashCode % _capacity];
        }

        [StructLayout(LayoutKind.Sequential, Size = CacheLineSize)]
        private struct Entry
        {
            public TKey key;     // Key of entry
            public TValue value; // Value of entry
            public Entry* next;
        }

        public struct Enumerator : IEnumerator<KeyValuePair<TKey, TValue>>, IDictionaryEnumerator
        {
            private FastDictionary<TKey, TValue> _dictionary;
            private int _index;
            private KeyValuePair<TKey, TValue> _current;
            private readonly int _getEnumeratorRetType;  // What should Enumerator.Current return?

            internal const int DictEntry = 1;
            internal const int KeyValuePair = 2;

            internal Enumerator(FastDictionary<TKey, TValue> dictionary, int getEnumeratorRetType)
            {
                _dictionary = dictionary;
                _index = 0;
                _getEnumeratorRetType = getEnumeratorRetType;
                _current = default;
            }

            public bool MoveNext()
            {
                // Use unsigned comparison since we set index to dictionary.count+1 when the enumeration ends.
                // dictionary.count+1 could be negative if dictionary.count is int.MaxValue
                while ((uint)_index < (uint)_dictionary._count)
                {
                    ref Entry entry = ref _dictionary._entries![_index++];
                    _current = new KeyValuePair<TKey, TValue>(entry.key, entry.value);
                    return true;
                }

                _index = _dictionary._count + 1;
                _current = default;
                return false;
            }

            public KeyValuePair<TKey, TValue> Current => _current;

            public void Dispose() { }

            object? IEnumerator.Current
            {
                get
                {
                    if (_index == 0 || (_index == _dictionary._count + 1))
                    {
                        ThrowHelper.ThrowInvalidOperationException_InvalidOperation_EnumOpCantHappen();
                    }

                    if (_getEnumeratorRetType == DictEntry)
                    {
                        return new DictionaryEntry(_current.Key, _current.Value);
                    }

                    return new KeyValuePair<TKey, TValue>(_current.Key, _current.Value);
                }
            }

            void IEnumerator.Reset()
            {
                _index = 0;
                _current = default;
            }

            DictionaryEntry IDictionaryEnumerator.Entry
            {
                get
                {
                    if (_index == 0 || (_index == _dictionary._count + 1))
                    {
                        ThrowHelper.ThrowInvalidOperationException_InvalidOperation_EnumOpCantHappen();
                    }

                    return new DictionaryEntry(_current.Key, _current.Value);
                }
            }

            object IDictionaryEnumerator.Key
            {
                get
                {
                    if (_index == 0 || (_index == _dictionary._count + 1))
                    {
                        ThrowHelper.ThrowInvalidOperationException_InvalidOperation_EnumOpCantHappen();
                    }

                    return _current.Key;
                }
            }

            object? IDictionaryEnumerator.Value
            {
                get
                {
                    if (_index == 0 || (_index == _dictionary._count + 1))
                    {
                        ThrowHelper.ThrowInvalidOperationException_InvalidOperation_EnumOpCantHappen();
                    }

                    return _current.Value;
                }
            }
        }

        private sealed class IDictionaryDebugView
        {
            private readonly IDictionary<TKey, TValue> _dict;

            public IDictionaryDebugView(IDictionary<TKey, TValue> dictionary)
            {
                ArgumentNullException.ThrowIfNull(dictionary);

                _dict = dictionary;
            }

            [DebuggerBrowsable(DebuggerBrowsableState.RootHidden)]
            public KeyValuePair<TKey, TValue>[] Items
            {
                get
                {
                    KeyValuePair<TKey, TValue>[] items = new KeyValuePair<TKey, TValue>[_dict.Count];
                    _dict.CopyTo(items, 0);
                    return items;
                }
            }
        }
    }


    internal static class HashHelpers
    {
        const uint c1 = 0xcc9e2d51;
        const uint c2 = 0x1b873593;

        // Licensed to the .NET Foundation under one or more agreements.
        // The .NET Foundation licenses this file to you under the MIT license.
        // See the LICENSE file in the project root for more information.

        public const int HashCollisionThreshold = 100;

        public const int HashPrime = 101;

        // Table of prime numbers to use as hash table sizes. 
        // A typical resize algorithm would pick the smallest prime number in this array
        // that is larger than twice the previous capacity. 
        // Suppose our Hashtable currently has capacity x and enough elements are added 
        // such that a resize needs to occur. Resizing first computes 2x then finds the 
        // first prime in the table greater than 2x, i.e. if primes are ordered 
        // p_1, p_2, ..., p_i, ..., it finds p_n such that p_n-1 < 2x < p_n. 
        // Doubling is important for preserving the asymptotic complexity of the 
        // hashtable operations such as add.  Having a prime guarantees that double 
        // hashing does not lead to infinite loops.  IE, your hash function will be 
        // h1(key) + i*h2(key), 0 <= i < size.  h2 and the size must be relatively prime.
        public static readonly int[] Primes = {
        3, 7, 11, 17, 23, 29, 37, 47, 59, 71, 89, 107, 131, 163, 197, 239, 293, 353, 431, 521, 631, 761, 919,
        1103, 1327, 1597, 1931, 2333, 2801, 3371, 4049, 4861, 5839, 7013, 8419, 10103, 12143, 14591,
        17519, 21023, 25229, 30293, 36353, 43627, 52361, 62851, 75431, 90523, 108631, 130363, 156437,
        187751, 225307, 270371, 324449, 389357, 467237, 560689, 672827, 807403, 968897, 1162687, 1395263,
        1674319, 2009191, 2411033, 2893249, 3471899, 4166287, 4999559, 5999471, 7199369};

        public static bool IsPrime(int candidate)
        {
            if ((candidate & 1) != 0)
            {
                int limit = (int)Math.Sqrt(candidate);
                for (int divisor = 3; divisor <= limit; divisor += 2)
                {
                    if ((candidate % divisor) == 0)
                        return false;
                }
                return true;
            }
            return (candidate == 2);
        }

        public static int GetPrime(int min)
        {
            for (int i = 0; i < Primes.Length; i++)
            {
                int prime = Primes[i];
                if (prime >= min) return prime;
            }

            //outside of our predefined table. 
            //compute the hard way. 
            for (int i = (min | 1); i < Int32.MaxValue; i += 2)
            {
                if (IsPrime(i) && ((i - 1) % HashPrime != 0))
                    return i;
            }
            return min;
        }

        // Returns size of hashtable to grow to.
        public static int ExpandPrime(int oldSize)
        {
            int newSize = 2 * oldSize;

            // Allow the hashtables to grow to maximum possible size (~2G elements) before encoutering capacity overflow.
            // Note that this check works even when _items.Length overflowed thanks to the (uint) cast
            if ((uint)newSize > MaxPrimeArrayLength && MaxPrimeArrayLength > oldSize)
            {
                Debug.Assert(MaxPrimeArrayLength == GetPrime(MaxPrimeArrayLength), "Invalid MaxPrimeArrayLength");
                return MaxPrimeArrayLength;
            }

            return GetPrime(newSize);
        }


        // This is the maximum prime smaller than Array.MaxArrayLength
        public const int MaxPrimeArrayLength = 0x7FEFFFFD;
    }

    internal static class ThrowHelper
    {
        [DoesNotReturn]
        public static void ThrowInvalidOperationException_InvalidOperation_EnumOpCantHappen()
        {
            throw new InvalidOperationException();
        }
    }
}