import time

def measure(task_name, func):
    start = time.time()
    result = func()
    end = time.time()

    duration = end - start

    with open("results.txt", "a", encoding="utf-8") as f:
        f.write("\n============================\n")
        f.write(f"{task_name}\n")
        f.write("Result:\n")

        # formatted output
        if isinstance(result, list):
            for i, (word, count) in enumerate(result, 1):
                f.write(f"{i:2}. {word:<20} {count:>10,}\n")
        else:
            f.write(str(result) + "\n")

        f.write(f"Time: {duration:.4f} sec\n")

    print(f"{task_name} Time: {duration:.4f} sec")