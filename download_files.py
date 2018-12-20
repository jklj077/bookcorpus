"""
download book files in the list and convert to txt
epub are converted to markdown
"""

import argparse
import json
import os
import sys
from glob import glob

import requests
import tqdm

import epub2txt

cj = requests.cookies.RequestsCookieJar()


parser = argparse.ArgumentParser()
parser.add_argument("--out-dir", "--out", type=str, default="out_txts")
parser.add_argument(
    "--list-path", "--list", type=str, default="ml_url_list.jsonl"
)
parser.add_argument("--trash-bad-count", action="store_true", default=True)
parser.add_argument(
    "--languages", "--langs", "--lang", nargs="+", type=str, default=["English"]
)
args = parser.parse_args()

SKIPS = ["Plays", "Screenplays"]


def write_txt(txt, out_path, num_words=None):
    # occasionally, some epubs text are decoded with errors
    # e.g. repeated bib lines
    # filter out them by comparing number of words
    counted_num_words = len(txt.split())
    if not txt.strip():
        pass
    elif num_words is None or (
        num_words * 0.5 < counted_num_words < num_words * 1.5
    ):
        with open(
            out_path, "w", encoding="utf8"
        ) as txt_out:  # convert epub2txt and save
            txt_out.write(txt)


def main():
    dataset = []
    out_dir = args.out_dir
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    filelist_path = args.list_path

    lines = list(open(filelist_path).readlines())

    done_files = set(
        [
            os.path.split(path)[-1]
            for path in glob(os.path.join(out_dir, "*.txt"))
        ]
    )
    sys.stderr.write(
        "{} files already had been saved in {}.\n".format(
            len(done_files), out_dir
        )
    )

    for i, line in enumerate(tqdm.tqdm(lines, ascii=True)):
        if not line.strip():
            continue
        # read data
        try:
            # {"page": "https://www.smashwords.com/books/view/52", "epub": "https://www.smashwords.com/books/download/52/8/latest/0/0/smashwords-style-guide.epub", "title": "Smashwords Style Guide", "author": "Mark Coker", "genres": ["Nonfiction\tComputers and Internet\tDigital publishing", "Nonfiction\tPublishing\tSelf-publishing"], "publish": "May 05, 2008", "num_words": 28300, "b_idx": 1}
            data = json.loads(line.strip())

            if "lang" not in data:
                raise Exception(
                    "Language filter is available "
                    "when the url list has lang information. "
                    "Please regenerate url list with the latest script."
                )
            if data["lang"] not in args.languages:
                continue

            skip = False

            for skip_genre in SKIPS:
                for genre in data["genres"]:
                    if skip_genre in genre:
                        skip = True
                        break
                if skip:
                    break

            if skip:
                continue

            _, book_id = os.path.split(data["page"])
            _, file_name = os.path.split(data["epub"])

            out_file_name = "{}__{}".format(
                book_id, file_name.replace(".epub", ".txt")
            )
            out_path = os.path.join(out_dir, out_file_name)
            if out_file_name in done_files:
                continue
            if data["txt"]:
                # try to download .txt file
                r = requests.get(data["txt"], cookies=cj)
                txt = r.text
                write_txt(txt, out_path, None)
            else:
                # revenge by converting .epub to .txt
                tmp_path = os.path.join(out_dir, file_name)
                r = requests.get(data["epub"], cookies=cj)
                with open(tmp_path, "wb") as tmp_f:
                    tmp_f.write(r.content)

                txt = epub2txt.epub2txt(tmp_path).convert()
                if args.trash_bad_count:
                    if "num_words" in data:
                        write_txt(txt, out_path, data["num_words"])
                else:
                    write_txt(txt, out_path, None)
        except Exception as e:
            sys.stderr.write(str(e) + "\n")
            if os.path.exists(out_path):
                os.remove(out_path)
        # remove .epub
        try:
            os.remove(tmp_path)
        except:
            pass


if __name__ == "__main__":
    main()
