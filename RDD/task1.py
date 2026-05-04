from common.parser import parse

class Task1:
    def run(self, sc, path):

        def to_tuple(x):
            # (min, max, sum, count)
            return (x, x, x, 1)

        def reduce_stats(a, b):
            return (
                min(a[0], b[0]),   # min
                max(a[1], b[1]),   # max
                a[2] + b[2],       # sum
                a[3] + b[3]        # count
            )

        result = (
            sc.textFile(path)
            .map(parse)
            .filter(lambda x: x is not None)
            .map(lambda x: int(x["size"]))
            .map(to_tuple)
            .reduce(reduce_stats)
        )

        min_size, max_size, total, count = result 
        avg_size = total / count if count != 0 else 0

        output = (
            f"Min size: {min_size}\n"
            f"Max size: {max_size}\n"
            f"Avg size: {avg_size}"
        )

        print(output)
        return output