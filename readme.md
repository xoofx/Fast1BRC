# 1️⃣🐝🏎️ The One Billion Row Challenge - .NET Edition

> The One Billion Row Challenge (1BRC [Original Java Challenge](https://github.com/gunnarmorling/1brc)) is a fun exploration of how far modern .NET can be pushed for aggregating one billion rows from a text file.
> Grab all your (virtual) threads, reach out to SIMD, optimize your GC, or pull any other trick, and create the fastest implementation for solving this task!

Aggregated results for C#/F# at https://github.com/praeclarum/1brc

**Fast1BRC** might be one of the fastest (if not the fastest! 😅) implementation in the wild west. 🚀

## Techniques used

- Multiple threads
- No memory mapped file but RandomAccess reopening the same handle per thread
  - As I discovered that it is lowering OS contention
- FNV-1A 64 bit hashing of the city names aligned on 8 bytes boundary, seems that it is not authorized from official rules, but I found it quite solid
  - It is using a vectorized version with `Vector128` which is able to hash a name in just a few SIMD instructions 
- No particular tricks for parsing the temperature, apart assuming that there is only 1 digit after the `.`

## Build

You need to have [.NET 8 SDK installed](https://dotnet.microsoft.com/en-us/download/dotnet/8.0)

```
dotnet publish -c Release -r win-x64
.\bin\Release\net8.0\win-x64\publish\Fast1BRC measurements.txt
```

To fully test, you need to generate `measurements.txt`, easier on Ubuntu/MacOS:

- Install OpenJDK 21 https://jdk.java.net/21/
- Install Maven 3.9+ https://maven.apache.org/download.cgi
- Clone `https://github.com/gunnarmorling/1brc` and go to its folder
- Run `mvn package` 
- Run `./create_measurements.sh 1000000000`
- Copy `measurements.txt` to a place where you can use it with Fast1BRC

## Results

`952.78 ms` on my machine (AMD Ryzen 7950X at 60W)


## License

This software is released under the [BSD-2-Clause license](https://opensource.org/licenses/BSD-2-Clause). 

## Author

Alexandre Mutel aka [xoofx](https://xoofx.com).
