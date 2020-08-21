from nozzle.dag import Dag, Operator
from nozzle.runtime import run_dag


def chop_veggies_fn(): print("chop veggies")
def steam_broccoli_fn(): print("steam broccoli")
def steam_broccoli_error_fn(): raise RuntimeError("Oops, we don't have a steaming basket!")
def cook_carrot_fn(): print("cook carrot")
def wash_dishes_fn(): print("wash dishes")


with Dag(id="dinner") as dag:
    chop_veggies = Operator(chop_veggies_fn)
    steam_broccoli = Operator(steam_broccoli_fn)
    cook_carrot = Operator(cook_carrot_fn)
    wash_dishes = Operator(wash_dishes_fn)
    chop_veggies >> [steam_broccoli, cook_carrot] >> wash_dishes

run_dag(dag, num_of_processes=4)
