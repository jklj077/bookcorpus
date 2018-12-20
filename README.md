# Homemade BookCorpus

[BookCorpus](http://yknzhu.wixsite.com/mbweb) is a popular text corpus, espetially for unsupervised learning of sentence encoders/decoders. But, usage of BookCorpus is limited.

This is an unofficial repository for collecting data from [smashwords.com](https://www.smashwords.com/books/category/1/downloads/0/free/medium/0), which is an original source.


## How to use

Prepare downloaded URLs.

```
python download_list.py --list-path <list-path>
```

Download their files. Download `txt` if possible. Otherwise, try to extract text from `epub`. `--trash-bad-count` filters out `epub` files whose word count is largely different from its official stat.

```
python download_files.py --list <list-path> --out out_txts --trash-bad-count --lang English
```

Make concatenated text with sentence-per-line format. And, tokenize them into segmented words.

```
python make_shards.py out_txts
```

## Requirement

- beautifulsoup4
- nltk
  - And, download tokenizers by `python -c "import nltk;nltk.download('punkt')"`
- spacy
- tqdm
- html2text


## Acknowledgement

`epub2txt.py` is derived and modified from https://github.com/kevinxiong/epub2txt/blob/master/epub2txt.py

