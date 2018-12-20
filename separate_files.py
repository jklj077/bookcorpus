"""
nltk and spacy deal terribly with books that are Plays or Screenplays, mainly because of the prompts and the scene descrptions.
This script is used to separate the downloaded txts so that the Plays and the Screenplays are excluded.
"""

import os
import sys
import tqdm
import json


def main():
    out_dir = "out_txts"
    move_dir = "skip_txts"

    if not os.path.exists(move_dir):
        os.makedirs(move_dir, exist_ok=True)

    filelist_path = "ml_url_list.jsonl"

    SKIPS = ["Plays", "Screenplays"]

    lines = list(open(filelist_path).readlines())

    total = 0
    done = 0
    move = 0

    progress_bar = tqdm.tqdm(lines, ascii=True)
    for i, line in enumerate(progress_bar):
        if not line.strip():
            continue
        data = json.loads(line.strip())

        _, book_id = os.path.split(data["page"])
        _, file_name = os.path.split(data["epub"])

        out_file_name = f"{book_id}__{file_name.replace('.epub', '.txt')}"
        out_path = os.path.join(out_dir, out_file_name)
        total += 1
        if not os.path.exists(out_path):
            continue
        done += 1

        skip = False

        for skip_genre in SKIPS:
            for genre in data["genres"]:
                if skip_genre in genre:
                    skip = True
                    break
            if skip:
                break

        if skip:
            os.rename(out_path, os.path.join(move_dir, out_file_name))
            move += 1

        progress_bar.set_postfix_str(
            f"total: {total}, done: {done}, move: {move}"
        )


if __name__ == "__main__":
    main()

