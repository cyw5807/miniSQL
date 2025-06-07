# MiniSQL

本框架参考CMU-15445 BusTub框架进行改写，在保留了缓冲池、索引、记录模块的一些核心设计理念的基础上，做了一些修改和扩展，使之兼容于原MiniSQL实验指导的要求。

### 编译&开发环境
- Apple clang version: 11.0+ (MacOS)，使用`gcc --version`和`g++ --version`查看
- gcc & g++ : 8.0+ (Linux)，使用`gcc --version`和`g++ --version`查看
- cmake: 3.20+ (Both)，使用`cmake --version`查看
- gdb: 7.0+ (Optional)，使用`gdb --version`查看
- flex & bison (暂时不需要安装，但如果需要对SQL编译器的语法进行修改，需要安装）
- llvm-symbolizer (暂时不需要安装)
    - in mac os `brew install llvm`, then set path and env variables.
    - in centos `yum install devtoolset-8-libasan-devel libasan`
    - https://www.jetbrains.com/help/clion/google-sanitizers.html#AsanChapter
    - https://www.jianshu.com/p/e4cbcd764783

### 构建
#### Windows
目前该代码暂不支持在Windows平台上的编译。但在Win10及以上的系统中，可以通过安装WSL（Windows的Linux子系统）来进行
开发和构建。WSL请选择Ubuntu子系统（推荐Ubuntu20及以上）。如果你使用Clion作为IDE，可以在Clion中配置WSL从而进行调试，具体请参考
[Clion with WSL](https://blog.jetbrains.com/clion/2018/01/clion-and-linux-toolchain-on-windows-are-now-friends/)

#### MacOS & Linux & WSL
基本构建命令
```bash
mkdir build
cd build
cmake ..
make -j
```
若不涉及到`CMakeLists`相关文件的变动且没有新增或删除`.cpp`代码（通俗来说，就是只是对现有代码做了修改）
则无需重新执行`cmake..`命令，直接执行`make -j`编译即可。

默认以`debug`模式进行编译，如果你需要使用`release`模式进行编译：
```bash
cmake -DCMAKE_BUILD_TYPE=Release ..
```

### 运行
在构建后，在`build`目录输入命令`./bin/main`运行我们的MiniSQL程序。