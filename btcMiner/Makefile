CCOMPILE=mpiCC
CFLAGS=
ARCHIVE=

all: btcMiner

pagerank: btcMiner
	$(CCOMPILE) btcMiner.cpp -o btcMiner -I ../../lib $(CFLAGS) $(ARCHIVE) -Wno-deprecated

clean:
	-rm btcMiner
