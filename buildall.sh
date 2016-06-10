cd ../h5spark
sbt assembly
cd ../large-scale-climate
rm lib/*
cp ../h5spark/target/scala-2.10/*.jar lib
cp ../h5spark/lib/* lib
sbt assembly
