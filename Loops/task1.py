from common.parser import parse


class Task1Loops:
    def run(self, path):
        min_size = None
        max_size = None
        total = 0
        count = 0

        with open(path, "r", encoding="utf-8") as file:
            for line in file:
                parsed = parse(line)

                if not parsed:
                    continue

                size = int(parsed["size"])

                if min_size is None:
                    min_size = size
                    max_size = size
                else:
                    if size < min_size:
                        min_size = size
                    if size > max_size:
                        max_size = size

                total += size
                count += 1

        avg_size = total / count if count > 0 else 0

        result = (
            f"Min size: {min_size}\n"
            f"Max size: {max_size}\n"
            f"Avg size: {avg_size}"
        )

        print(result)
        return result