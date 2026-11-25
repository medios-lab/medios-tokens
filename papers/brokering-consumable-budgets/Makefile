.PHONY: pdf clean

pdf: main.pdf

main.pdf: main.tex refs.bib
	tectonic main.tex

clean:
	rm -f main.pdf main.aux main.bbl main.blg main.log main.out
