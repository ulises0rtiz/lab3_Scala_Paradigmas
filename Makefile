.PHONY: all compile run clean

# El comando por defecto cuando se ejecuta 'make'
all: run

# Ejecuta todo forzando bash para que no pierda la ruta de sbt
run:
	@bash -c 'JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 PATH="/usr/lib/jvm/java-17-openjdk-amd64/bin:$$PATH" SBT_OPTS="--add-exports=java.base/sun.nio.ch=ALL-UNNAMED" sbt compile run'

# Limpia los binarios
clean:
	@bash -c 'JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 PATH="/usr/lib/jvm/java-17-openjdk-amd64/bin:$$PATH" SBT_OPTS="--add-exports=java.base/sun.nio.ch=ALL-UNNAMED" sbt clean'