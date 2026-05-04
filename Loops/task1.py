from common.parser import parse


class Task1Loops:
    def run(self, path):
        file = open(path, "r", encoding="utf-8")

        min_size = None
        max_size = None
        total = 0
        count = 0
        for line in file:   #processing one line at a time
            parsed = parse(line)

            if not parsed:
                continue
            size = parsed["size"]
            if min_size is None:
                min_size = size
                max_size = size

            if size < min_size:
                min_size = size

            if size > max_size:
                max_size = size

            total += size
            count += 1

        file.close()

        avg_size = total / count if count > 0 else 0

        result = (
            f"Min size: {min_size}\n"
            f"Max size: {max_size}\n"
            f"Avg size: {avg_size}"
        )
        print(result)
        return result