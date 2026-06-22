.PHONY: all compile run clean

# Configuración estricta de entorno para Apache Spark con Java 17
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH:=$(JAVA_HOME)/bin:$(PATH)
export SBT_OPTS=--add-exports=java.base/sun.nio.ch=ALL-UNNAMED

# El comando por defecto cuando se ejecuta simplemente 'make'
all: run

# Compila y ejecuta con un único comando, como pide la consigna
run:
	sbt compile run

# Limpia los binarios generados
clean:
	sbt clean