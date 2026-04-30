from Loops.task1 import Task1Loops
from common.timer import measure

path = "data/data.out"

print("\n LOOPS Results: \n")

measure("LOOPS Task1 measure", lambda: Task1Loops().run(path))

