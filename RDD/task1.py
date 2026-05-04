from common.parser import parse

class Task1:
    def run(self, sc, path):
        data = sc.textFile(path)

        parsed = data.map(parse).filter(lambda x: x is not None)
        sizes = parsed.map(lambda x: int(x["size"]))

        result = sizes.map(lambda x: (x, x, x, 1)) \
                        .reduce(lambda a, b: (
                            min(a[0], b[0]),   # min
                            max(a[1], b[1]),   # max
                            a[2] + b[2],       # sum
                            a[3] + b[3]        # count
                        ))

        min_size, max_size, total, count = result
        avg_size = total / count if count != 0 else 0

        output = (
            f"Min size: {min_size}\n"
            f"Max size: {max_size}\n"
            f"Avg size: {avg_size}"
        )

        print(output)
        return output