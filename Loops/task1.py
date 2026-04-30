class Task1Loops:

    def run(self, path):

        sizes = []

        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                parts = line.split()
                if len(parts) < 4:
                    continue
                try:
                    sizes.append(int(parts[3]))
                except:
                    continue

        print("Min size:", min(sizes))
        print("Max size:", max(sizes))
        print("Avg size:", sum(sizes) / len(sizes))