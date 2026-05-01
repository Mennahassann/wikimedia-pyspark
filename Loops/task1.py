from common.parser import parse


class Task1Loops:
    def run(self, path):
        sizes = []

        with open(path, "r", encoding="utf-8") as file:
            for line in file:
                parsed = parse(line)
                if parsed:
                    sizes.append(parsed["size"])

        min_size = min(sizes)
        max_size = max(sizes)
        avg_size = sum(sizes) / len(sizes)

        result = f"Min size: {min_size}\nMax size: {max_size}\nAvg size: {avg_size}"

        print(result)
        return result