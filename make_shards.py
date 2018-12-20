"""
This script is used to merge the downloaded txts, re-segment the sentences, tokenize the sentences, and split the sentences into shards.
The sentence segmentation uses nltk, as spacy uses parsing to segment the sentence, which is too costly.
The tokenization uses spacy.
Sentences that have fewer than 2 tokens or have more than 128 tokens are filtered.
Every 1,000,000 sentences form a shard.
"""

import os
import sys
from glob import glob

import re
import ftfy

from nltk.tokenize import sent_tokenize

import spacy

from tqdm import tqdm
import multiprocessing


def worker(in_q, out_q, rank, tqdm_lock):

    tqdm.set_lock(tqdm_lock)

    nlp = spacy.load(
        "en_core_web_sm", disable=["parser", "tagger", "ner", "textcat"]
    )

    while True:
        file_path = in_q.get()
        if file_path is None:
            break

        sents, n_sent = convert_into_sentences(
            open(file_path, "r", encoding="utf8").readlines()
        )

        processed_sents = []

        for sent in tqdm(
            sents,
            desc=f"{os.path.basename(file_path)[:20]: <20}",
            position=rank,
            ascii=True,
            dynamic_ncols=True,
        ):
            sent = sent.strip()
            if not sent:
                continue
            sent = text_standardize(ftfy.fix_text(sent))
            if len(sent) > 8192:
                continue
            sent = nlp(sent)
            if len(sent) <= 2 or len(sent) >= 128:
                continue
            sent = " ".join([token.text.lower() for token in sent])

            if purge_sent(sent):
                continue

            processed_sents.append(sent)

        out_q.put((file_path, processed_sents))


def convert_into_sentences(lines):
    """
     because the format of the text is realy inconsistent,
     some are markdown formatted, and a sentence may spread over multiple lines.
     to deal with it, the lines are joined unless more than 2 blank lines are seen,
     and the sentences are re-segmented
     """

    blank = 0
    stack = []
    sent_L = []
    n_sent = 0

    for chunk in lines:
        if not chunk.strip():
            blank += 1
            if blank >= 2:
                if stack:
                    sents = sent_tokenize(
                        " ".join(stack).strip().replace("\n", " ")
                    )
                    sent_L.extend(sents)
                    n_sent += len(sents)
                    sent_L.append("\n")
                    stack = []
                blank = 0
            continue
        stack.append(chunk.strip())

    if stack:
        sents = sent_tokenize(" ".join(stack).strip().replace("\n", " "))
        sent_L.extend(sents)
        n_sent += len(sents)
    return sent_L, n_sent


def text_standardize(text):
    """
    fixes some issues the spacy tokenizer had on books corpus
    also does some whitespace standardization
    """
    text = text.replace("—", "-")
    text = text.replace("–", "-")
    text = text.replace("―", "-")
    text = text.replace("…", "...")
    text = text.replace("´", "'")
    text = re.sub(
        r"""(-+|~+|!+|"+|;+|\?+|\++|,+|\)+|\(+|\\+|\/+|\*+|\[+|\]+|}+|{+|\|+|_+)""",
        r" \1 ",
        text,
    )
    text = text.replace("_", "")  # added to deal with markdown
    text = re.sub(r"\s*\n\s*", " \n ", text)
    text = re.sub(r"[^\S\n]+", " ", text)
    text = text.replace(
        "\n", " "
    )  # added since nltk sometimes keeps \n in a sentence
    return text.strip()


def purge_sent(sent):
    """
    filter the tokenized sentences to exclude:
        copyright preface
        table of content
        html
        markdown
    because they are not natural sentences
    the strategy is very aggresive
    """

    words_list = [
        "chapter",
        "smashwords",
        "< /",
        "/ >",
        "www.",
        "isbn",
        "copyright",
        "all rights reserved",
        ".png",
        ".html",
        ".org",
        ".com",
        "©",
    ]
    starts_token = ["#", "_", "*", "[", "part"]

    for word in words_list:
        if word in sent:
            return True

    for token in starts_token:
        if sent.startswith(token):
            return True

    return False


def multiprocess_main():
    """
    using multiple processes to process the txts
    with nice tqdm progress bars
    about two hours on my 16-core computer to process more than 10,000 books
    """
    multiprocessing.freeze_support()
    n_process = multiprocessing.cpu_count() - 1

    in_queue = multiprocessing.Queue()
    out_queue = multiprocessing.Queue()
    lock = multiprocessing.RLock()

    file_dir = "out_txts"
    file_list = list(sorted(glob(os.path.join(file_dir, "*.txt"))))
    out_dir = "out_shards"

    if not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    for file_path in file_list:
        in_queue.put(file_path)
    for _ in range(n_process):
        in_queue.put(None)

    processes = []
    for i in range(n_process):
        p = multiprocessing.Process(
            target=worker, args=(in_queue, out_queue, i + 1, lock)
        )
        p.start()
        processes.append(p)

    shard = 0
    count = 0

    fout = open(
        os.path.join(out_dir, f"book_corpus_{shard:02d}.txt"),
        "w",
        encoding="utf8",
    )

    with tqdm(
        total=len(file_list), ascii=True, dynamic_ncols=True, position=0
    ) as pbar:

        for i in range(len(file_list)):
            file_path, processed_sentences = out_queue.get()

            for sent in processed_sentences:
                print(sent, file=fout)
                count += 1

                if count % 1_000_000 == 0:
                    shard += 1
                    fout.close()
                    fout = open(
                        os.path.join(out_dir, f"book_corpus_{shard:02d}.txt"),
                        "w",
                        encoding="utf8",
                    )

            pbar.update(1)
            pbar.set_postfix_str(
                f"shard={shard:02d}, count={count%1_000_000:06n}, i={i}, file={os.path.basename(file_path)[:20]: <20}"
            )
        fout.close()

    for i, p in enumerate(processes):
        p.join()
        print(f"join process {i}")


def main():
    """
    using one process to process the txts
    with nice tqdm progress bars
    about 8 to process more than 10,000 books
    """
    file_dir = "out_txts"

    i = 9975
    shard = 8
    count = 8_819_244

    file_list = list(sorted(glob(os.path.join(file_dir, "*.txt"))))

    nlp = spacy.load(
        "en_core_web_sm", disable=["parser", "tagger", "ner", "textcat"]
    )

    fout = open("book_corpus_{:02d}.txt".format(shard), "w", encoding="utf8")

    progress_bar = tqdm(file_list, ascii=True, dynamic_ncols=True)

    for i, file_path in enumerate(progress_bar):
        sents, n_sent = convert_into_sentences(
            open(file_path, "r", encoding="utf8").readlines()
        )

        for sent in tqdm(sents, ascii=True, dynamic_ncols=True):
            sent = sent.strip()
            if not sent:
                continue
            sent = text_standardize(ftfy.fix_text(sent))
            sent = nlp(sent)
            if len(sent) <= 2 or len(sent) >= 100:
                continue
            sent = " ".join([token.text.lower() for token in sent])

            if purge_sent(sent):
                continue

            print(sent, file=fout)
            count += 1

            if count % 1_000_000 == 0:
                shard += 1
                fout.close()
                fout = open(
                    "book_corpus_{:02d}.txt".format(shard), "w", encoding="utf8"
                )

        progress_bar.set_postfix_str(
            f"shard={shard:02d}, count={count%1000000:09d}, i={i}"
        )


if __name__ == "__main__":
    multiprocess_main()
