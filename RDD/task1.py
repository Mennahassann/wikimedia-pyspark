from common.parser import parse


class Task1:
    def run(self, sc, path):
        data = sc.textFile(path)

        parsed = data.map(parse).filter(lambda x: x is not None)
        sizes = parsed.map(lambda x: int(x["size"]))

        min_size = sizes.min()
        max_size = sizes.max()
        avg_size = sizes.mean()

        result = f"Min size: {min_size}\nMax size: {max_size}\nAvg size: {avg_size}"

        print(result)
        return result