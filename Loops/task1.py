from pyspark import AccumulatorParam
from common.parser import parse

class StatsAccumulator(AccumulatorParam):
    def zero(self, value):
        return (float("inf"), float("-inf"), 0, 0)

    def addInPlace(self, acc1, acc2):
        min1, max1, sum1, count1 = acc1
        min2, max2, sum2, count2 = acc2

        return (
            min(min1, min2),
            max(max1, max2),
            sum1 + sum2,
            count1 + count2
        )


class Task1Loops:
    def run(self, sc, path):

        rdd = sc.textFile(path)

        stats_acc = sc.accumulator(
            (float("inf"), float("-inf"), 0, 0),
            StatsAccumulator()
        )

        def process_partition(iterator):
            local_min = float("inf")
            local_max = float("-inf")
            local_sum = 0
            local_count = 0

            for line in iterator:
                parsed = parse(line)
                if not parsed:
                    continue

                size = int(parsed["size"])

                local_min = min(local_min, size)
                local_max = max(local_max, size)
                local_sum += size
                local_count += 1

            stats_acc.add((local_min, local_max, local_sum, local_count))

        rdd.foreachPartition(process_partition)

        min_size, max_size, total, count = stats_acc.value
        avg = total / count if count else 0

        result = (
            f"Min size: {min_size}\n"
            f"Max size: {max_size}\n"
            f"Avg size: {avg}"
        )

        print(result)
        return result