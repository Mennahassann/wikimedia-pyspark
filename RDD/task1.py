from common.parser import parse

class Task1:

    def run(self, sc, path):

        data = sc.textFile(path)

        parsed = data.map(parse).filter(lambda x: x is not None)

        sizes = parsed.map(lambda x: x["size"])

        print("Min size:", sizes.min())
        print("Max size:", sizes.max())
        print("Avg size:", sizes.mean())

        print(sizes.count())