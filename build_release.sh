rm -rf cpp-jni java-dist java-jni cpp/debug
mkdir cpp/debug
cd cpp/debug

arch -x86_64 cmake -DCMAKE_BUILD_TYPE=RELEASE -DARROW_GANDIVA=ON -DARROW_JEMALLOC=OFF -DARROW_GANDIVA_JAVA=ON -DARROW_BUILD_TESTS=OFF ..
arch -x86_64 make -j 8 
if [ $? -ne 0 ]
then
  echo "failed"
  exit 1
fi

cd ../../
mkdir -p java-jni cpp-jni

arch -x86_64 cmake -S cpp  -B cpp-jni  -DARROW_BUILD_SHARED=OFF  -DARROW_JEMALLOC=OFF -DARROW_CSV=ON  -DARROW_DATASET=ON  -DARROW_DEPENDENCY_SOURCE=BUNDLED  -DARROW_DEPENDENCY_USE_SHARED=OFF  -DARROW_FILESYSTEM=ON  -DARROW_GANDIVA=ON  -DARROW_GANDIVA_STATIC_LIBSTDCPP=ON  -DARROW_ORC=ON  -DARROW_PARQUET=ON  -DARROW_S3=ON  -DARROW_USE_CCACHE=ON  -DCMAKE_BUILD_TYPE=RELEASE  -DCMAKE_INSTALL_LIBDIR=lib/x86_64  -DCMAKE_INSTALL_PREFIX=java-dist  -DCMAKE_UNITY_BUILD=ON
arch -x86_64 cmake --build cpp-jni --target install --config Release
if [ $? -ne 0 ]
then
  echo "failed"
  exit 1
fi

arch -x86_64 cmake -S java -B java-jni -DARROW_JAVA_JNI_ENABLE_C=OFF -DARROW_JEMALLOC=OFF -DARROW_JAVA_JNI_ENABLE_DEFAULT=ON -DBUILD_TESTING=OFF -DCMAKE_BUILD_TYPE=RELEASE -DCMAKE_INSTALL_LIBDIR=lib/x86_64 -DCMAKE_INSTALL_PREFIX=java-dist -DCMAKE_PREFIX_PATH=$PWD/java-dist/lib/x86_64/cmake
arch -x86_64 cmake --build java-jni --target install --config Release
if [ $? -ne 0 ]
then
  echo "failed"
  exit 1
fi

cd java
/opt/homebrew/bin/mvn -DskipTests -Darrow.c.jni.dist.dir=/Users/logan.riggs/github/arrow-fork/arrow/java-dist/lib -Darrow.cpp.build.dir=/Users/logan.riggs/github/arrow-fork/arrow/java-dist/lib -Parrow-jni clean install
