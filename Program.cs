// Copyright (c) Alexandre Mutel. All rights reserved.
// Licensed under the BSD-Clause 2 license.
// See license.txt file in the project root for full license information.
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Text;

namespace Fast1BRC;

/// <summary>
/// C# version for The One Billion Row Challenge
/// https://github.com/gunnarmorling/1brc
/// Author: xoofx
/// </summary>
internal static unsafe class Program
{
    private const int ReadBufferSize = 512 << 10; // 512 KB
        
    static void Main(string[] args)
    {
        var filePath = args.Length > 0 ? args[0] : Path.Combine(Environment.CurrentDirectory, "measurements-1_000_000-sample.txt");
        if (!File.Exists(filePath)) throw new FileNotFoundException(filePath);

        var count = args.Contains("--pgo") ? 10 : 1;
        for (int i = 0; i < count; i++)
        {
            var clock = Stopwatch.StartNew();
            Console.OutputEncoding = Encoding.UTF8;
            Console.WriteLine(Run(filePath));
            clock.Stop();

            Console.WriteLine($"Elapsed in {clock.Elapsed.TotalMilliseconds} ms");
        }
    }


    /// <summary>
    /// Main entry point for the program
    /// </summary>
    /// <param name="filePath">File to process</param>
    /// <returns>Formatted results</returns>
    private static string Run(string filePath)
    {
        using var fileHandle = File.OpenHandle(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, FileOptions.SequentialScan);
        var fileLength = RandomAccess.GetLength(fileHandle);

        // --------------------------------------------------------------------
        // Split the file by chunks and process them in parallel
        // --------------------------------------------------------------------
        Span<byte> localBuffer = stackalloc byte[256];
        var taskCount = Math.Min((int)(fileLength / (ReadBufferSize * 4) + 1), Environment.ProcessorCount);
        var tasks = new List<Task<Dictionary<ulong, EntryItem>>>(taskCount);
        var chunkSize = fileLength / taskCount;
        long startOffset = 0;
        for (int i = 0; i < taskCount; i++)
        {
            long endOffset;
            if (i + 1 < taskCount)
            {
                endOffset = (i + 1) * chunkSize;

                // We always align to the end of the line
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
            tasks.Add(Task.Run(() => ProcessChunk(filePath, localStartOffset, endOffset)));
            startOffset = endOffset;
        }
        Task.WaitAll(tasks.ToArray());

        // --------------------------------------------------------------------
        // Aggregate the results
        // --------------------------------------------------------------------
        var result = tasks.Select(x => x.Result).SelectMany(x => x.Values).Aggregate(
            new Dictionary<string, EntryItem>(),
            (acc, item) =>
            {
                string? name = item.Name!;
                ref var existingValue = ref CollectionsMarshal.GetValueRefOrAddDefault(acc, name, out _);
                existingValue.Name = name;
                existingValue.AggregateFrom(ref item);
                return acc;
            }
        );

        // --------------------------------------------------------------------
        // Format the results
        // --------------------------------------------------------------------
        var builder = new StringBuilder();
        builder.Append('{');
        bool isFirst = true;
        foreach (var entryItem in result.Values.OrderBy(x => (string)x.Name!, StringComparer.Ordinal))
        {
            if (!isFirst) builder.Append(", ");
            builder.Append($"{entryItem.Name}={(entryItem.MinTemp / 10.0):0.0}/{entryItem.SumTemp / (10.0 * entryItem.Count):0.0}/{(entryItem.MaxTemp / 10.0):0.0}");
            isFirst = false;
        }
        builder.Append('}');

        return builder.ToString();
    }

    /// <summary>
    /// Process a chunk of the file
    /// </summary>
    /// <param name="filePath">The file to process</param>
    /// <param name="startOffset">The starting offset within the file. Always start at the beginning of a new line.</param>
    /// <param name="endOffsetNotInclusive">The end offset - non inclusive.</param>
    /// <returns></returns>
    private static Dictionary<ulong, EntryItem> ProcessChunk(string filePath, long startOffset, long endOffsetNotInclusive)
    {
        // Reopen the file to improve concurrency, as it seems - at least on Windows - that it is more efficient to have multiple handles
        using var localFileHandle = File.OpenHandle(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, FileOptions.SequentialScan);
        var buffer = GC.AllocateUninitializedArray<byte>(ReadBufferSize);
        ref var pBuffer = ref MemoryMarshal.GetArrayDataReference(buffer);
        var entries = new Dictionary<ulong, EntryItem>(11000);

        long fileOffset = startOffset;
        nint bufferOffset = 0; // in case we have a line that is not fully read
        while (fileOffset < endOffsetNotInclusive)
        {
            // Read the next buffer
            var maxLengthToRead = Math.Min(ReadBufferSize - bufferOffset, endOffsetNotInclusive - fileOffset);
            var span = MemoryMarshal.CreateSpan(ref Unsafe.Add(ref pBuffer, bufferOffset), (int)maxLengthToRead);
            var bufferLength = RandomAccess.Read(localFileHandle, span, fileOffset);

            if (bufferLength == 0) break;
            fileOffset += bufferLength;
            bufferLength += (int)bufferOffset;

            // Process the buffer
            var startLineIndex = ProcessBuffer(entries, ref pBuffer, bufferLength);

            if (startLineIndex >= 0)
            {
                // Copy the remaining bytes to the beginning of the buffer (A line that was not fully read)
                var remainingLength = bufferLength - (int)startLineIndex;
                Unsafe.CopyBlockUnaligned(ref pBuffer, ref Unsafe.Add(ref pBuffer, startLineIndex), (uint)remainingLength);
                bufferOffset = remainingLength;
            }
            else
            {
                bufferOffset = 0;
            }
        }

        return entries;
    }

    /// <summary>
    /// Process a buffer
    /// </summary>
    /// <param name="entries">The dictionary to add entries.</param>
    /// <param name="pBuffer">The start of the buffer.</param>
    /// <param name="bufferLength">The length of the buffer</param>
    /// <returns>An index to the remaining buffer that hasn't been processed because the line was not complete; otherwise -1</returns>
    private static nint ProcessBuffer(Dictionary<ulong, EntryItem> entries, ref byte pBuffer, nint bufferLength)
    {
        nint index = 0;
        while (index < bufferLength)
        {
            nint startLineIndex = index;

            // --------------------------------------------------------------------
            // Process the name
            // We calculate the FNV-1A hash as we go
            // With a 64bit hash, we should avoid any collisions for the entries that we have
            // --------------------------------------------------------------------
            nint commaIndex;
            ulong hash = 0xcbf29ce484222325;
            const ulong fnv1APrime = 0x100000001b3;

            // We vectorize the calculation of the hash up to 16 bytes
            if (Vector128.IsHardwareAccelerated)
            {
                while (index + Vector128<byte>.Count < bufferLength)
                {
                    var mask = Vector128.Create((byte)';');
                    var v = Vector128.LoadUnsafe(ref Unsafe.Add(ref pBuffer, index));
                    var eq = Vector128.Equals(v, mask);
                    if (eq == Vector128<byte>.Zero)
                    {
                        // If we don't have a match it means that the string is longer than 16 bytes
                        // but we can hash the first 16 bytes
                        hash = (hash ^ v.AsUInt64().GetElement(0)) * fnv1APrime;
                        hash = (hash ^ v.AsUInt64().GetElement(1)) * fnv1APrime;
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
                        // We hash the value (as we would do in the following loop)
                        hash = (hash ^ v.AsUInt64().GetElement(0)) * fnv1APrime;
                        hash = (hash ^ v.AsUInt64().GetElement(1)) * fnv1APrime;
                        index += offset;
                        goto readTemp;
                    }
                }
            }

            ulong acc = 0;
            int accCount = 0;
            int accTotal8BytesCount = 0;
            while (index < bufferLength)
            {
                var c = Unsafe.Add(ref pBuffer, index);
                if (c == (byte)';')
                {
                    if (accCount > 0)
                    {
                        hash = (hash ^ acc) * fnv1APrime;
                        accTotal8BytesCount++;
                    }

                    // If we have an odd number of bytes, we add a 0 byte to the hash
                    // To match the Vector128 version
                    if ((accTotal8BytesCount & 1) != 0)
                    {
                        hash = (hash ^ 0) * fnv1APrime;
                    }
                    goto readTemp;
                }
                acc |=  ((ulong)c << (accCount * 8));
                accCount++;
                if (accCount == 8)
                {
                    hash = (hash ^ acc) * fnv1APrime;
                    acc = 0;
                    accCount = 0;
                    accTotal8BytesCount++;
                }
                index++;
            }

            return startLineIndex;

        readTemp:
            commaIndex = index++;

            // --------------------------------------------------------------------
            // Process the temperature
            // --------------------------------------------------------------------
            int sign = 1;
            int temp = 0;
            while (index < bufferLength)
            {
                var c = Unsafe.Add(ref pBuffer, index++);
                if (c == (byte)'-')
                {
                    sign = -1;
                }
                else if (char.IsAsciiDigit((char)c))
                {
                    temp = temp * 10 + (c - '0');
                }
                else if (c == (byte)'\n')
                {
                    temp *= sign;
                    goto proceedEntry;
                }
            }

            return startLineIndex;

            proceedEntry:

            // --------------------------------------------------------------------
            // Add the entry
            // --------------------------------------------------------------------
            ref var entry = ref CollectionsMarshal.GetValueRefOrAddDefault(entries, hash, out var exists);
            if (!exists)
            {
                var name = Encoding.UTF8.GetString(MemoryMarshal.CreateReadOnlySpan(ref Unsafe.Add(ref pBuffer, startLineIndex), (int)(commaIndex - startLineIndex)));
                entry.Name = name;
                entry.MinTemp = int.MaxValue;
            }
            entry.Count++;
            entry.SumTemp += temp;
            entry.MinTemp = Math.Min(entry.MinTemp, temp);
            entry.MaxTemp = Math.Max(entry.MaxTemp, temp);
        }

        return -1;
    }

    /// <summary>
    /// Structure to hold the entry data per city
    /// </summary>
    private struct EntryItem
    {
        public string? Name;
        public int Count;
        public long SumTemp;
        public int MinTemp;
        public int MaxTemp;

        public void AggregateFrom(ref EntryItem item)
        {
            SumTemp += item.SumTemp;
            Count += item.Count;
            MinTemp = Math.Min(item.MinTemp, MinTemp);
            MaxTemp = Math.Max(item.MaxTemp, MaxTemp);
        }
    }
}