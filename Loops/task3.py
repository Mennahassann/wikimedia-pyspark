import re
import time
from collections import defaultdict
from pyspark import AccumulatorParam

class DictAccumulatorParam(AccumulatorParam):

    def zero(self, value):
        return defaultdict(int)

    def addInPlace(self, acc1, acc2):
        for k, v in acc2.items():
            acc1[k] += v
        return acc1


class LoopTask3:

    def process_partition(self, partition):

        local_dict = defaultdict(int)

        for line in partition:

            parts = line.split(" ")

            if len(parts) < 2:
                continue

            title = parts[1]

            title = title.lower()

            title = re.sub(r'[^a-zA-Z0-9]', '_', title)

            words = title.split("_")

            for word in words:

                if word != "":
                    local_dict[word] += 1

        self.word_acc.add(local_dict)

    def run(self, sc, path):

        start_time = time.time()

        lines = sc.textFile(path)

        self.word_acc = sc.accumulator(
            defaultdict(int),
            DictAccumulatorParam()
        )

        lines.foreachPartition(self.process_partition)

        top10 = sorted(
            self.word_acc.value.items(),
            key=lambda x: x[1],
            reverse=True
        )[:10]

        end_time = time.time()

        print("Top 10 most frequent terms:")

        for i, (word, count) in enumerate(top10, 1):
            print(f"{i:2}. {word:<20} {count:>10,}")

        return end_time - start_time
