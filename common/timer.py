import time


def measure(task_name, func):
    start = time.time()
    result = func()
    end = time.time()

    duration = end - start

    with open("results.txt", "a", encoding="utf-8") as f:
        f.write(f"\n{task_name} \n")
        f.write(str(result) + "\n")
        f.write(f"Time: {duration:.4f} sec\n")

    print(f"{task_name} Time: {duration:.4f} sec")