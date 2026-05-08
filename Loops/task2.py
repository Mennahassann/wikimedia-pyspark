from pyspark import AccumulatorParam
from common.parser import parse


class ImagePageAccumulator(AccumulatorParam):
    def zero(self, value):
        return (0, 0)

    def addInPlace(self, acc1, acc2):
        total1, not_en1 = acc1
        total2, not_en2 = acc2

        return (total1 + total2, not_en1 + not_en2)


class Task2Loops:
    def run(self, sc, path):
        image_acc = sc.accumulator((0, 0), ImagePageAccumulator())

        image_extensions = (".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp")

        def process_partition(iterator):
            total_pages_ends_with_img = 0
            not_en_project = 0

            for line in iterator:
                parsed = parse(line)
                if not parsed:
                    continue

                if parsed["title"].lower().endswith(image_extensions):
                    total_pages_ends_with_img += 1
                    if parsed["project"] != "en":
                        not_en_project += 1

            image_acc.add((total_pages_ends_with_img, not_en_project))

        sc.textFile(path).foreachPartition(process_partition)

        total_pages_ends_with_img, not_en_project = image_acc.value
        result = (
            f"Total pages that end with image extensions: {total_pages_ends_with_img}\n"
            f"Pages that end with image extensions and are not in the 'en' project: {not_en_project}"
        )

        print(result)
        return result