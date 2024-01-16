using System;
using System.Collections;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Fast1BRC;

[DebuggerTypeProxy(typeof(FastDictionary<,>.IDictionaryDebugView))]
[DebuggerDisplay("Count = {Count}")]
public class FastDictionary<TKey, TValue> : IDictionary<TKey, TValue> where TKey : IEquatable<TKey>
{
    private int[]? _buckets;
    private Entry[]? _entries;
    private int _count;
    private int _freeList;
    private int _freeCount;
    private const int StartOfFreeList = -3;

    public FastDictionary() : this(0) { }

    public FastDictionary(int capacity)
    {
        if (capacity < 0)
        {
            ThrowHelper.ThrowArgumentOutOfRangeException(ExceptionArgument.capacity);
        }

        if (capacity > 0)
        {
            Initialize(capacity);
        }
    }

    public FastDictionary(IDictionary<TKey, TValue> dictionary) :
        this(dictionary?.Count ?? 0)
    {
        if (dictionary == null)
        {
            ThrowHelper.ThrowArgumentNullException(ExceptionArgument.dictionary);
        }

        AddRange(dictionary!);
    }

    public FastDictionary(IEnumerable<KeyValuePair<TKey, TValue>> collection) :
        this((collection as ICollection<KeyValuePair<TKey, TValue>>)?.Count ?? 0)
    {
        if (collection == null)
        {
            ThrowHelper.ThrowArgumentNullException(ExceptionArgument.collection);
        }

        AddRange(collection!);
    }

    private void AddRange(IEnumerable<KeyValuePair<TKey, TValue>> enumerable)
    {
        // We similarly special-case KVP<>[] and List<KVP<>>, as they're commonly used to seed dictionaries, and
        // we want to avoid the enumerator costs (e.g. allocation) for them as well. Extract a span if possible.
        ReadOnlySpan<KeyValuePair<TKey, TValue>> span;
        if (enumerable is KeyValuePair<TKey, TValue>[] array)
        {
            span = array;
        }
        else if (enumerable.GetType() == typeof(List<KeyValuePair<TKey, TValue>>))
        {
            span = CollectionsMarshal.AsSpan((List<KeyValuePair<TKey, TValue>>)enumerable);
        }
        else
        {
            // Fallback path for all other enumerables
            foreach (KeyValuePair<TKey, TValue> pair in enumerable)
            {
                Add(pair.Key, pair.Value);
            }
            return;
        }

        // We got a span. Add the elements to the dictionary.
        foreach (KeyValuePair<TKey, TValue> pair in span)
        {
            Add(pair.Key, pair.Value);
        }
    }

    public int Count => _count - _freeCount;

    public KeyCollection Keys => new (this);

    ICollection<TKey> IDictionary<TKey, TValue>.Keys => Keys;

    public ValueCollection Values => new (this);

    ICollection<TValue> IDictionary<TKey, TValue>.Values => Values;

    public TValue this[TKey key]
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            ref TValue value = ref FindValue(key);
            if (!Unsafe.IsNullRef(ref value))
            {
                return value;
            }

            ThrowHelper.ThrowKeyNotFoundException(key);
            return default;
        }
        set
        {
            bool modified = TryInsert(key, value, InsertionBehavior.OverwriteExisting);
            Debug.Assert(modified);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Add(TKey key, TValue value)
    {
        bool modified = TryInsert(key, value, InsertionBehavior.ThrowOnExisting);
        Debug.Assert(modified); // If there was an existing key and the Add failed, an exception will already have been thrown.
    }

    public ref TValue GetValueRefOrNullRef(TKey key, out bool exists)
    {
        // NOTE: this method is mirrored in CollectionsMarshal.GetValueRefOrAddDefault below.
        // If you make any changes here, make sure to keep that version in sync as well.

        if (key == null)
        {
            ThrowHelper.ThrowArgumentNullException(ExceptionArgument.key);
        }

        if (_buckets == null)
        {
            Initialize(0);
        }
        Debug.Assert(_buckets != null);

        Entry[]? entries = _entries;
        Debug.Assert(entries != null, "expected entries to be non-null");

        uint hashCode = (uint)(key!.GetHashCode());

        ref int bucket = ref GetBucket(hashCode);
        int i = bucket - 1; // Value in _buckets is 1-based

        ref Entry entry = ref Unsafe.NullRef<Entry>();

        while (true)
        {
            // Should be a while loop https://github.com/dotnet/runtime/issues/9422
            // Test uint in if rather than loop condition to drop range check for following array access
            if ((uint)i >= (uint)entries.Length)
            {
                break;
            }

            entry = ref entries[i];

            if (entry.hashCode == hashCode && entry.key.Equals(key))
            {
                exists = true;
                return ref entry.value;
            }

            i = entry.next;
        }

        exists = false;
        return ref Unsafe.NullRef<TValue>();
    }


    public ref TValue GetValueRefOrAddDefault(TKey key, out bool exists)
    {
        // NOTE: this method is mirrored in CollectionsMarshal.GetValueRefOrAddDefault below.
        // If you make any changes here, make sure to keep that version in sync as well.

        if (key == null)
        {
            ThrowHelper.ThrowArgumentNullException(ExceptionArgument.key);
        }

        if (_buckets == null)
        {
            Initialize(0);
        }
        Debug.Assert(_buckets != null);

        Entry[]? entries = _entries;
        Debug.Assert(entries != null, "expected entries to be non-null");

        uint hashCode = (uint)(key!.GetHashCode());

        ref int bucket = ref GetBucket(hashCode);
        int i = bucket - 1; // Value in _buckets is 1-based

        ref Entry entry = ref Unsafe.NullRef<Entry>();

        while (true)
        {
            // Should be a while loop https://github.com/dotnet/runtime/issues/9422
            // Test uint in if rather than loop condition to drop range check for following array access
            if ((uint)i >= (uint)entries.Length)
            {
                break;
            }

            entry = ref entries[i];

            if (entry.hashCode == hashCode && entry.key.Equals(key))
            {
                exists = true;
                return ref entry.value;
            }

            i = entry.next;
        }

        int index;
        if (_freeCount > 0)
        {
            index = _freeList;
            Debug.Assert((StartOfFreeList - entries[_freeList].next) >= -1, "shouldn't overflow because `next` cannot underflow");
            _freeList = StartOfFreeList - entries[_freeList].next;
            _freeCount--;
        }
        else
        {
            int count = _count;
            if (count == entries.Length)
            {
                Resize();
                bucket = ref GetBucket(hashCode);
            }
            index = count;
            _count = count + 1;
            entries = _entries;
        }

        entry = ref entries![index];
        entry.hashCode = hashCode;
        entry.next = bucket - 1; // Value in _buckets is 1-based
        entry.key = key;
        entry.value = default!;
        bucket = index + 1; // Value in _buckets is 1-based

        exists = false;
        return ref entry.value;
    }

    void ICollection<KeyValuePair<TKey, TValue>>.Add(KeyValuePair<TKey, TValue> keyValuePair) =>
        Add(keyValuePair.Key, keyValuePair.Value);

    bool ICollection<KeyValuePair<TKey, TValue>>.Contains(KeyValuePair<TKey, TValue> keyValuePair)
    {
        ref TValue value = ref FindValue(keyValuePair.Key);
        if (!Unsafe.IsNullRef(ref value) && EqualityComparer<TValue>.Default.Equals(value, keyValuePair.Value))
        {
            return true;
        }

        return false;
    }

    bool ICollection<KeyValuePair<TKey, TValue>>.Remove(KeyValuePair<TKey, TValue> keyValuePair)
    {
        ref TValue value = ref FindValue(keyValuePair.Key);
        if (!Unsafe.IsNullRef(ref value) && EqualityComparer<TValue>.Default.Equals(value, keyValuePair.Value))
        {
            Remove(keyValuePair.Key);
            return true;
        }

        return false;
    }

    public void Clear()
    {
        int count = _count;
        if (count > 0)
        {
            Debug.Assert(_buckets != null, "_buckets should be non-null");
            Debug.Assert(_entries != null, "_entries should be non-null");

            Array.Clear(_buckets);

            _count = 0;
            _freeList = -1;
            _freeCount = 0;
            Array.Clear(_entries, 0, count);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool ContainsKey(TKey key) =>
        !Unsafe.IsNullRef(ref FindValue(key));

    public bool ContainsValue(TValue value)
    {
        Entry[]? entries = _entries;
        if (value == null)
        {
            for (int i = 0; i < _count; i++)
            {
                if (entries![i].next >= -1 && entries[i].value == null)
                {
                    return true;
                }
            }
        }
        else if (typeof(TValue).IsValueType)
        {
            // ValueType: Devirtualize with EqualityComparer<TValue>.Default intrinsic
            for (int i = 0; i < _count; i++)
            {
                if (entries![i].next >= -1 && EqualityComparer<TValue>.Default.Equals(entries[i].value, value))
                {
                    return true;
                }
            }
        }
        else
        {
            // Object type: Shared Generic, EqualityComparer<TValue>.Default won't devirtualize
            // https://github.com/dotnet/runtime/issues/10050
            // So cache in a local rather than get EqualityComparer per loop iteration
            EqualityComparer<TValue> defaultComparer = EqualityComparer<TValue>.Default;
            for (int i = 0; i < _count; i++)
            {
                if (entries![i].next >= -1 && defaultComparer.Equals(entries[i].value, value))
                {
                    return true;
                }
            }
        }

        return false;
    }

    private void CopyTo(KeyValuePair<TKey, TValue>[] array, int index)
    {
        if (array == null)
        {
            ThrowHelper.ThrowArgumentNullException(ExceptionArgument.array);
        }

        if ((uint)index > (uint)array!.Length)
        {
            ThrowHelper.ThrowIndexArgumentOutOfRange_NeedNonNegNumException();
        }

        if (array.Length - index < Count)
        {
            ThrowHelper.ThrowArgumentException(ThrowHelper.Arg_ArrayPlusOffTooSmall);
        }

        int count = _count;
        Entry[]? entries = _entries;
        for (int i = 0; i < count; i++)
        {
            if (entries![i].next >= -1)
            {
                array[index++] = new KeyValuePair<TKey, TValue>(entries[i].key, entries[i].value);
            }
        }
    }

    public Enumerator GetEnumerator() => new Enumerator(this, Enumerator.KeyValuePair);

    IEnumerator<KeyValuePair<TKey, TValue>> IEnumerable<KeyValuePair<TKey, TValue>>.GetEnumerator() =>
        Count == 0 ? Enumerable.Empty<KeyValuePair<TKey, TValue>>().GetEnumerator() :
        GetEnumerator();

    private ref TValue FindValue(TKey key)
    {
        if (key == null)
        {
            ThrowHelper.ThrowArgumentNullException(ExceptionArgument.key);
        }

        if (_buckets != null)
        {
            Debug.Assert(_entries != null, "expected entries to be != null");
            uint hashCode = (uint)key!.GetHashCode();
            int i = GetBucket(hashCode);
            Entry[]? entries = _entries;
            ref Entry entry = ref Unsafe.NullRef<Entry>();

            // ValueType: Devirtualize with EqualityComparer<TKey>.Default intrinsic
            i--; // Value in _buckets is 1-based; subtract 1 from i. We do it here so it fuses with the following conditional.
            do
            {
                // Should be a while loop https://github.com/dotnet/runtime/issues/9422
                // Test in if to drop range check for following array access
                if ((uint)i >= (uint)entries.Length)
                {
                    break;
                }

                entry = ref entries[i];
                if (entry.hashCode == hashCode && entry.key.Equals(key))
                {
                    return ref entry.value;
                }

                i = entry.next;

            } while (true);
        }

        return ref Unsafe.NullRef<TValue>();
    }

    private int Initialize(int capacity)
    {
        int size = HashHelpers.GetPrime(capacity);
        int[] buckets = new int[size];
        Entry[] entries = new Entry[size];

        // Assign member variables after both arrays allocated to guard against corruption from OOM if second fails
        _freeList = -1;
        _buckets = buckets;
        _entries = entries;

        return size;
    }

    private bool TryInsert(TKey key, TValue value, InsertionBehavior behavior)
    {
        // NOTE: this method is mirrored in CollectionsMarshal.GetValueRefOrAddDefault below.
        // If you make any changes here, make sure to keep that version in sync as well.

        if (key == null)
        {
            ThrowHelper.ThrowArgumentNullException(ExceptionArgument.key);
        }

        if (_buckets == null)
        {
            Initialize(0);
        }
        Debug.Assert(_buckets != null);

        Entry[]? entries = _entries;
        Debug.Assert(entries != null, "expected entries to be non-null");

        uint hashCode = (uint)(key!.GetHashCode());

        ref int bucket = ref GetBucket(hashCode);
        int i = bucket - 1; // Value in _buckets is 1-based

        ref Entry entry = ref Unsafe.NullRef<Entry>();

        while (true)
        {
            // Should be a while loop https://github.com/dotnet/runtime/issues/9422
            // Test uint in if rather than loop condition to drop range check for following array access
            if ((uint)i >= (uint)entries.Length)
            {
                break;
            }

            entry = ref entries[i];

            if (entry.hashCode == hashCode && entry.key.Equals(key))
            {
                if (behavior == InsertionBehavior.OverwriteExisting)
                {
                    entry.value = value;
                    return true;
                }

                if (behavior == InsertionBehavior.ThrowOnExisting)
                {
                    ThrowHelper.ThrowAddingDuplicateWithKeyArgumentException(key);
                }

                return false;
            }

            i = entry.next;
        }

        int index;
        if (_freeCount > 0)
        {
            index = _freeList;
            Debug.Assert((StartOfFreeList - entries[_freeList].next) >= -1, "shouldn't overflow because `next` cannot underflow");
            _freeList = StartOfFreeList - entries[_freeList].next;
            _freeCount--;
        }
        else
        {
            int count = _count;
            if (count == entries.Length)
            {
                Resize();
                bucket = ref GetBucket(hashCode);
            }
            index = count;
            _count = count + 1;
            entries = _entries;
        }

        entry = ref entries![index];
        entry.hashCode = hashCode;
        entry.next = bucket - 1; // Value in _buckets is 1-based
        entry.key = key;
        entry.value = value;
        bucket = index + 1; // Value in _buckets is 1-based

        return true;
    }

    private void Resize() => Resize(HashHelpers.ExpandPrime(_count), false);

    private void Resize(int newSize, bool forceNewHashCodes)
    {
        // Value types never rehash
        Debug.Assert(!forceNewHashCodes || !typeof(TKey).IsValueType);
        Debug.Assert(_entries != null, "_entries should be non-null");
        Debug.Assert(newSize >= _entries.Length);

        Entry[] entries = new Entry[newSize];

        int count = _count;
        Array.Copy(_entries, entries, count);

        if (!typeof(TKey).IsValueType && forceNewHashCodes)
        {
            for (int i = 0; i < count; i++)
            {
                if (entries[i].next >= -1)
                {
                    entries[i].hashCode = (uint)entries[i].key.GetHashCode();
                }
            }
        }

        // Assign member variables after both arrays allocated to guard against corruption from OOM if second fails
        _buckets = new int[newSize];
        for (int i = 0; i < count; i++)
        {
            if (entries[i].next >= -1)
            {
                ref int bucket = ref GetBucket(entries[i].hashCode);
                entries[i].next = bucket - 1; // Value in _buckets is 1-based
                bucket = i + 1;
            }
        }

        _entries = entries;
    }

    public bool Remove(TKey key)
    {
        // The overload Remove(TKey key, out TValue value) is a copy of this method with one additional
        // statement to copy the value for entry being removed into the output parameter.
        // Code has been intentionally duplicated for performance reasons.

        if (key == null)
        {
            ThrowHelper.ThrowArgumentNullException(ExceptionArgument.key);
        }

        if (_buckets != null)
        {
            Debug.Assert(_entries != null, "entries should be non-null");

            uint hashCode = (uint)(key!.GetHashCode());

            ref int bucket = ref GetBucket(hashCode);
            Entry[]? entries = _entries;
            int last = -1;
            int i = bucket - 1; // Value in buckets is 1-based
            while (i >= 0)
            {
                ref Entry entry = ref entries[i];

                if (entry.hashCode == hashCode && entry.key.Equals(key))
                {
                    if (last < 0)
                    {
                        bucket = entry.next + 1; // Value in buckets is 1-based
                    }
                    else
                    {
                        entries[last].next = entry.next;
                    }

                    Debug.Assert((StartOfFreeList - _freeList) < 0, "shouldn't underflow because max hashtable length is MaxPrimeArrayLength = 0x7FEFFFFD(2146435069) _freelist underflow threshold 2147483646");
                    entry.next = StartOfFreeList - _freeList;

                    if (RuntimeHelpers.IsReferenceOrContainsReferences<TKey>())
                    {
                        entry.key = default!;
                    }

                    if (RuntimeHelpers.IsReferenceOrContainsReferences<TValue>())
                    {
                        entry.value = default!;
                    }

                    _freeList = i;
                    _freeCount++;
                    return true;
                }

                last = i;
                i = entry.next;
            }
        }
        return false;
    }

    public bool Remove(TKey key, [MaybeNullWhen(false)] out TValue value)
    {
        // This overload is a copy of the overload Remove(TKey key) with one additional
        // statement to copy the value for entry being removed into the output parameter.
        // Code has been intentionally duplicated for performance reasons.

        if (key == null)
        {
            ThrowHelper.ThrowArgumentNullException(ExceptionArgument.key);
        }

        if (_buckets != null)
        {
            Debug.Assert(_entries != null, "entries should be non-null");

            uint hashCode = (uint)(key!.GetHashCode());

            ref int bucket = ref GetBucket(hashCode);
            Entry[]? entries = _entries;
            int last = -1;
            int i = bucket - 1; // Value in buckets is 1-based
            while (i >= 0)
            {
                ref Entry entry = ref entries[i];

                if (entry.hashCode == hashCode && entry.key.Equals(key))
                {
                    if (last < 0)
                    {
                        bucket = entry.next + 1; // Value in buckets is 1-based
                    }
                    else
                    {
                        entries[last].next = entry.next;
                    }

                    value = entry.value;

                    Debug.Assert((StartOfFreeList - _freeList) < 0, "shouldn't underflow because max hashtable length is MaxPrimeArrayLength = 0x7FEFFFFD(2146435069) _freelist underflow threshold 2147483646");
                    entry.next = StartOfFreeList - _freeList;

                    if (RuntimeHelpers.IsReferenceOrContainsReferences<TKey>())
                    {
                        entry.key = default!;
                    }

                    if (RuntimeHelpers.IsReferenceOrContainsReferences<TValue>())
                    {
                        entry.value = default!;
                    }

                    _freeList = i;
                    _freeCount++;
                    return true;
                }

                last = i;
                i = entry.next;
            }
        }

        value = default;
        return false;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryGetValue(TKey key, [MaybeNullWhen(false)] out TValue value)
    {
        ref TValue valRef = ref FindValue(key);
        if (!Unsafe.IsNullRef(ref valRef))
        {
            value = valRef;
            return true;
        }

        value = default;
        return false;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryAdd(TKey key, TValue value) =>
        TryInsert(key, value, InsertionBehavior.None);

    bool ICollection<KeyValuePair<TKey, TValue>>.IsReadOnly => false;

    void ICollection<KeyValuePair<TKey, TValue>>.CopyTo(KeyValuePair<TKey, TValue>[] array, int index) =>
        CopyTo(array, index);

    IEnumerator IEnumerable.GetEnumerator() => ((IEnumerable<KeyValuePair<TKey, TValue>>)this).GetEnumerator();

    /// <summary>
    /// Ensures that the dictionary can hold up to 'capacity' entries without any further expansion of its backing storage
    /// </summary>
    public int EnsureCapacity(int capacity)
    {
        if (capacity < 0)
        {
            ThrowHelper.ThrowArgumentOutOfRangeException(ExceptionArgument.capacity);
        }

        int currentCapacity = _entries == null ? 0 : _entries.Length;
        if (currentCapacity >= capacity)
        {
            return currentCapacity;
        }

        if (_buckets == null)
        {
            return Initialize(capacity);
        }

        int newSize = HashHelpers.GetPrime(capacity);
        Resize(newSize, forceNewHashCodes: false);
        return newSize;
    }

    /// <summary>
    /// Sets the capacity of this dictionary to what it would be if it had been originally initialized with all its entries
    /// </summary>
    /// <remarks>
    /// This method can be used to minimize the memory overhead
    /// once it is known that no new elements will be added.
    ///
    /// To allocate minimum size storage array, execute the following statements:
    ///
    /// dictionary.Clear();
    /// dictionary.TrimExcess();
    /// </remarks>
    public void TrimExcess() => TrimExcess(Count);

    /// <summary>
    /// Sets the capacity of this dictionary to hold up 'capacity' entries without any further expansion of its backing storage
    /// </summary>
    /// <remarks>
    /// This method can be used to minimize the memory overhead
    /// once it is known that no new elements will be added.
    /// </remarks>
    public void TrimExcess(int capacity)
    {
        if (capacity < Count)
        {
            ThrowHelper.ThrowArgumentOutOfRangeException(ExceptionArgument.capacity);
        }

        int newSize = HashHelpers.GetPrime(capacity);
        Entry[]? oldEntries = _entries;
        int currentCapacity = oldEntries == null ? 0 : oldEntries.Length;
        if (newSize >= currentCapacity)
        {
            return;
        }

        int oldCount = _count;
        Initialize(newSize);

        Debug.Assert(oldEntries is not null);

        CopyEntries(oldEntries, oldCount);
    }

    private void CopyEntries(Entry[] entries, int count)
    {
        Debug.Assert(_entries is not null);

        Entry[] newEntries = _entries;
        int newCount = 0;
        for (int i = 0; i < count; i++)
        {
            uint hashCode = entries[i].hashCode;
            if (entries[i].next >= -1)
            {
                ref Entry entry = ref newEntries[newCount];
                entry = entries[i];
                ref int bucket = ref GetBucket(hashCode);
                entry.next = bucket - 1; // Value in _buckets is 1-based
                bucket = newCount + 1;
                newCount++;
            }
        }

        _count = newCount;
        _freeCount = 0;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ref int GetBucket(uint hashCode)
    {
        int[] buckets = _buckets!;
        return ref buckets[(uint)hashCode % buckets.Length];
    }

    private struct Entry
    {
        public uint hashCode;
        /// <summary>
        /// 0-based index of next entry in chain: -1 means end of chain
        /// also encodes whether this entry _itself_ is part of the free list by changing sign and subtracting 3,
        /// so -2 means end of free list, -3 means index 0 but on free list, -4 means index 1 but on free list, etc.
        /// </summary>
        public int next;
        public TKey key;     // Key of entry
        public TValue value; // Value of entry
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

                if (entry.next >= -1)
                {
                    _current = new KeyValuePair<TKey, TValue>(entry.key, entry.value);
                    return true;
                }
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

    [DebuggerTypeProxy(typeof(FastDictionary<,>.DictionaryKeyCollectionDebugView))]
    [DebuggerDisplay("Count = {Count}")]
    public struct KeyCollection : ICollection<TKey>, ICollection, IReadOnlyCollection<TKey>
    {
        private FastDictionary<TKey, TValue> _dictionary;

        public KeyCollection(FastDictionary<TKey, TValue> dictionary)
        {
            _dictionary = dictionary;
        }

        public Enumerator GetEnumerator() => new Enumerator(_dictionary);

        public void CopyTo(TKey[] array, int index)
        {
            if (array == null)
            {
                ThrowHelper.ThrowArgumentNullException(ExceptionArgument.array);
            }

            if (index < 0 || index > array!.Length)
            {
                ThrowHelper.ThrowIndexArgumentOutOfRange_NeedNonNegNumException();
            }

            if (array.Length - index < _dictionary.Count)
            {
                ThrowHelper.ThrowArgumentException(ThrowHelper.Arg_ArrayPlusOffTooSmall);
            }

            int count = _dictionary._count;
            Entry[]? entries = _dictionary._entries;
            for (int i = 0; i < count; i++)
            {
                if (entries![i].next >= -1) array[index++] = entries[i].key;
            }
        }

        public int Count => _dictionary.Count;

        bool ICollection<TKey>.IsReadOnly => true;

        void ICollection<TKey>.Add(TKey item) =>
            ThrowHelper.ThrowNotSupportedException(ThrowHelper.NotSupported_KeyCollectionSet);

        void ICollection<TKey>.Clear() =>
            ThrowHelper.ThrowNotSupportedException(ThrowHelper.NotSupported_KeyCollectionSet);

        public bool Contains(TKey item) =>
            _dictionary.ContainsKey(item);

        bool ICollection<TKey>.Remove(TKey item)
        {
            ThrowHelper.ThrowNotSupportedException(ThrowHelper.NotSupported_KeyCollectionSet);
            return false;
        }

        IEnumerator<TKey> IEnumerable<TKey>.GetEnumerator() =>
            Count == 0 ? Enumerable.Empty<TKey>().GetEnumerator() :
            GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => ((IEnumerable<TKey>)this).GetEnumerator();

        void ICollection.CopyTo(Array array, int index)
        {
            if (array == null)
            {
                ThrowHelper.ThrowArgumentNullException(ExceptionArgument.array);
            }

            if (array!.Rank != 1)
            {
                ThrowHelper.ThrowArgumentException(ThrowHelper.Arg_RankMultiDimNotSupported);
            }

            if (array.GetLowerBound(0) != 0)
            {
                ThrowHelper.ThrowArgumentException(ThrowHelper.Arg_NonZeroLowerBound);
            }

            if ((uint)index > (uint)array.Length)
            {
                ThrowHelper.ThrowIndexArgumentOutOfRange_NeedNonNegNumException();
            }

            if (array.Length - index < _dictionary.Count)
            {
                ThrowHelper.ThrowArgumentException(ThrowHelper.Arg_ArrayPlusOffTooSmall);
            }

            if (array is TKey[] keys)
            {
                CopyTo(keys, index);
            }
            else
            {
                object[]? objects = array as object[];
                if (objects == null)
                {
                    ThrowHelper.ThrowArgumentException_Argument_IncompatibleArrayType();
                }

                int count = _dictionary._count;
                Entry[]? entries = _dictionary._entries;
                try
                {
                    for (int i = 0; i < count; i++)
                    {
                        if (entries![i].next >= -1) objects[index++] = entries[i].key;
                    }
                }
                catch (ArrayTypeMismatchException)
                {
                    ThrowHelper.ThrowArgumentException_Argument_IncompatibleArrayType();
                }
            }
        }

        bool ICollection.IsSynchronized => false;

        object ICollection.SyncRoot => null!;

        public struct Enumerator : IEnumerator<TKey>, IEnumerator
        {
            private FastDictionary<TKey, TValue> _dictionary;
            private int _index;
            private TKey? _currentKey;

            internal Enumerator(FastDictionary<TKey, TValue> dictionary)
            {
                _dictionary = dictionary;
                _index = 0;
                _currentKey = default;
            }

            public void Dispose() { }

            public bool MoveNext()
            {
                while ((uint)_index < (uint)_dictionary._count)
                {
                    ref Entry entry = ref _dictionary._entries![_index++];

                    if (entry.next >= -1)
                    {
                        _currentKey = entry.key;
                        return true;
                    }
                }

                _index = _dictionary._count + 1;
                _currentKey = default;
                return false;
            }

            public TKey Current => _currentKey!;

            object? IEnumerator.Current
            {
                get
                {
                    if (_index == 0 || (_index == _dictionary._count + 1))
                    {
                        ThrowHelper.ThrowInvalidOperationException_InvalidOperation_EnumOpCantHappen();
                    }

                    return _currentKey;
                }
            }

            void IEnumerator.Reset()
            {
                _index = 0;
                _currentKey = default;
            }
        }
    }

    [DebuggerTypeProxy(typeof(FastDictionary<,>.DictionaryValueCollectionDebugView))]
    [DebuggerDisplay("Count = {Count}")]
    public struct ValueCollection : ICollection<TValue>, ICollection, IReadOnlyCollection<TValue>
    {
        private FastDictionary<TKey, TValue> _dictionary;

        public ValueCollection(FastDictionary<TKey, TValue> dictionary)
        {
            _dictionary = dictionary;
        }

        public Enumerator GetEnumerator() => new Enumerator(_dictionary);

        public void CopyTo(TValue[] array, int index)
        {
            if (array == null)
            {
                ThrowHelper.ThrowArgumentNullException(ExceptionArgument.array);
            }

            if ((uint)index > array!.Length)
            {
                ThrowHelper.ThrowIndexArgumentOutOfRange_NeedNonNegNumException();
            }

            if (array.Length - index < _dictionary.Count)
            {
                ThrowHelper.ThrowArgumentException(ThrowHelper.Arg_ArrayPlusOffTooSmall);
            }

            int count = _dictionary._count;
            Entry[]? entries = _dictionary._entries;
            for (int i = 0; i < count; i++)
            {
                if (entries![i].next >= -1) array[index++] = entries[i].value;
            }
        }

        public int Count => _dictionary.Count;

        bool ICollection<TValue>.IsReadOnly => true;

        void ICollection<TValue>.Add(TValue item) =>
            ThrowHelper.ThrowNotSupportedException(ThrowHelper.NotSupported_ValueCollectionSet);

        bool ICollection<TValue>.Remove(TValue item)
        {
            ThrowHelper.ThrowNotSupportedException(ThrowHelper.NotSupported_ValueCollectionSet);
            return false;
        }

        void ICollection<TValue>.Clear() =>
            ThrowHelper.ThrowNotSupportedException(ThrowHelper.NotSupported_ValueCollectionSet);

        bool ICollection<TValue>.Contains(TValue item) => _dictionary.ContainsValue(item);

        IEnumerator<TValue> IEnumerable<TValue>.GetEnumerator() =>
            Count == 0 ? Enumerable.Empty<TValue>().GetEnumerator() :
            GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => ((IEnumerable<TValue>)this).GetEnumerator();

        void ICollection.CopyTo(Array array, int index)
        {
            if (array == null)
            {
                ThrowHelper.ThrowArgumentNullException(ExceptionArgument.array);
            }

            if (array!.Rank != 1)
            {
                ThrowHelper.ThrowArgumentException(ThrowHelper.Arg_RankMultiDimNotSupported);
            }

            if (array.GetLowerBound(0) != 0)
            {
                ThrowHelper.ThrowArgumentException(ThrowHelper.Arg_NonZeroLowerBound);
            }

            if ((uint)index > (uint)array.Length)
            {
                ThrowHelper.ThrowIndexArgumentOutOfRange_NeedNonNegNumException();
            }

            if (array.Length - index < _dictionary.Count)
            {
                ThrowHelper.ThrowArgumentException(ThrowHelper.Arg_ArrayPlusOffTooSmall);
            }

            if (array is TValue[] values)
            {
                CopyTo(values, index);
            }
            else
            {
                object[]? objects = array as object[];
                if (objects == null)
                {
                    ThrowHelper.ThrowArgumentException_Argument_IncompatibleArrayType();
                }

                int count = _dictionary._count;
                Entry[]? entries = _dictionary._entries;
                try
                {
                    for (int i = 0; i < count; i++)
                    {
                        if (entries![i].next >= -1) objects[index++] = entries[i].value!;
                    }
                }
                catch (ArrayTypeMismatchException)
                {
                    ThrowHelper.ThrowArgumentException_Argument_IncompatibleArrayType();
                }
            }
        }

        bool ICollection.IsSynchronized => false;

        object ICollection.SyncRoot => null!;

        public struct Enumerator : IEnumerator<TValue>, IEnumerator
        {
            private readonly FastDictionary<TKey, TValue> _dictionary;
            private int _index;
            private TValue? _currentValue;

            internal Enumerator(FastDictionary<TKey, TValue> dictionary)
            {
                _dictionary = dictionary;
                _index = 0;
                _currentValue = default;
            }

            public void Dispose() { }

            public bool MoveNext()
            {
                while ((uint)_index < (uint)_dictionary._count)
                {
                    ref Entry entry = ref _dictionary._entries![_index++];

                    if (entry.next >= -1)
                    {
                        _currentValue = entry.value;
                        return true;
                    }
                }
                _index = _dictionary._count + 1;
                _currentValue = default;
                return false;
            }

            public TValue Current => _currentValue!;

            object? IEnumerator.Current
            {
                get
                {
                    if (_index == 0 || (_index == _dictionary._count + 1))
                    {
                        ThrowHelper.ThrowInvalidOperationException_InvalidOperation_EnumOpCantHappen();
                    }

                    return _currentValue;
                }
            }

            void IEnumerator.Reset()
            {
                _index = 0;
                _currentValue = default;
            }
        }
    }

    /// <summary>
    /// Used internally to control behavior of insertion into a <see cref="Dictionary{TKey, TValue}"/> or <see cref="HashSet{T}"/>.
    /// </summary>
    private enum InsertionBehavior : byte
    {
        /// <summary>
        /// The default insertion behavior.
        /// </summary>
        None = 0,

        /// <summary>
        /// Specifies that an existing entry with the same key should be overwritten if encountered.
        /// </summary>
        OverwriteExisting = 1,

        /// <summary>
        /// Specifies that if an existing entry with the same key is encountered, an exception should be thrown.
        /// </summary>
        ThrowOnExisting = 2
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

    private sealed class DictionaryKeyCollectionDebugView
    {
        private readonly ICollection<TKey> _collection;

        public DictionaryKeyCollectionDebugView(ICollection<TKey> collection)
        {
            ArgumentNullException.ThrowIfNull(collection);

            _collection = collection;
        }

        [DebuggerBrowsable(DebuggerBrowsableState.RootHidden)]
        public TKey[] Items
        {
            get
            {
                TKey[] items = new TKey[_collection.Count];
                _collection.CopyTo(items, 0);
                return items;
            }
        }
    }

    private sealed class DictionaryValueCollectionDebugView
    {
        private readonly ICollection<TValue> _collection;

        public DictionaryValueCollectionDebugView(ICollection<TValue> collection)
        {
            ArgumentNullException.ThrowIfNull(collection);

            _collection = collection;
        }

        [DebuggerBrowsable(DebuggerBrowsableState.RootHidden)]
        public TValue[] Items
        {
            get
            {
                TValue[] items = new TValue[_collection.Count];
                _collection.CopyTo(items, 0);
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
        if (min < 0)
            ThrowHelper.ThrowArgumentException(ExceptionArgument.min);

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
    public const string Arg_RankMultiDimNotSupported = "Only single dimensional arrays are supported for the requested action";
    public const string NotSupported_KeyCollectionSet = "Mutating a key collection derived from a dictionary is not allowed.";
    public const string Arg_ArrayPlusOffTooSmall = "Destination array is not long enough to copy all the items in the collection. Check array index and length";
    public const string Arg_NonZeroLowerBound = "The lower bound of target array must be zero.";
    public const string NotSupported_ValueCollectionSet = "Mutating a value collection derived from a dictionary is not allowed.";

    [Conditional("DEBUG")]
    public static void CheckOutOfRange(int index, int length)
    {
        if ((uint)index >= (uint)length) throw new IndexOutOfRangeException($"Index {index} is out of range of length {length}");
    }

    [DoesNotReturn]
    public static void ThrowIndexOutOfRangeException(int index)
    {
        throw new IndexOutOfRangeException($"Index {index} is out of range");
    }

    [DoesNotReturn]
    public static void ThrowIndexOutOfRangeException(nint index)
    {
        throw new IndexOutOfRangeException($"Index {index} is out of range");
    }

    public static void ThrowArgumentOutOfRangeException(ExceptionArgument ex)
    {
        throw GetArgumentOutOfRangeException(ex);
    }

    public static void ThrowArgumentNullException(ExceptionArgument ex)
    {
        throw GetArgumentNullException(ex);
    }

    private static ArgumentOutOfRangeException GetArgumentOutOfRangeException(ExceptionArgument argument)
    {
        return new ArgumentOutOfRangeException(GetArgumentName(argument));
    }

    private static ArgumentNullException GetArgumentNullException(ExceptionArgument argument)
    {
        return new ArgumentNullException(GetArgumentName(argument));
    }

    // This function will convert an ExceptionArgument enum value to the argument name string.
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static string GetArgumentName(ExceptionArgument argument)
    {
        Debug.Assert(Enum.IsDefined(typeof(ExceptionArgument), argument), "The enum value is not defined, please check the ExceptionArgument Enum.");
        return argument.ToString();
    }

    [DoesNotReturn]
    public static void ThrowNotSupportedException(string notSupportedKeyCollectionSet)
    {
        throw new NotSupportedException(notSupportedKeyCollectionSet);
    }

    [DoesNotReturn]
    public static void ThrowInvalidOperationException_InvalidOperation_EnumOpCantHappen()
    {
        throw new InvalidOperationException();
    }

    [DoesNotReturn]
    public static void ThrowKeyNotFoundException<TKey>(TKey key)
    {
        throw new KeyNotFoundException($"Key {key} was not found");
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    [DoesNotReturn]
    public static void ThrowInvalidCurrentContainer()
    {
        throw new InvalidOperationException("Cannot add a child. The current container is not set.");
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    [DoesNotReturn]
    public static void ThrowInvalidContainer(ExceptionArgument argument)
    {
        throw new ArgumentException("The node is not a container", GetArgumentName(argument));
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    [DoesNotReturn]
    public static void ThrowCannotPopCurrentContainer()
    {
        throw new InvalidOperationException($"Cannot pop current container. The current container is null or root.");
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    [DoesNotReturn]
    public static void ThrowKeyAlreadyInserted(ExceptionArgument argName, string name)
    {
        throw new ArgumentException($"Invalid key {name} is duplicated", GetArgumentName(argName));
    }

    [DoesNotReturn]
    public static void ThrowArgumentException(ExceptionArgument argArrayPlusOffTooSmall)
    {
        throw new NotImplementedException();
    }

    [DoesNotReturn]
    public static void ThrowArgumentException(string message)
    {
        throw new ArgumentException(message);
    }

    [DoesNotReturn]
    public static void ThrowIndexArgumentOutOfRange_NeedNonNegNumException()
    {
        throw new IndexOutOfRangeException();
    }

    [DoesNotReturn]
    public static void ThrowAddingDuplicateWithKeyArgumentException<TKey>(TKey key)
    {
        throw new ArgumentException($"Invalid key {key} duplicated");
    }

    [DoesNotReturn]
    public static void ThrowInvalidOperationSystemParameters()
    {
        throw new InvalidOperationException("The SystemParameters cannot be retrieved");
    }

    [DoesNotReturn]
    internal static void ThrowArgumentException_Argument_IncompatibleArrayType()
    {
        throw new ArgumentException("Target array type is not compatible with the type of items in the collection.");
    }
}

internal enum ExceptionArgument
{
    name,
    parent,
    index,
    element,
    item,
    array,
    value,
    builder,
    capacity,
    dictionary,
    collection,
    key,
    min,
    Arg_ArrayPlusOffTooSmall,
    other,
    match
}
