#import pandas as pd
#
#df = pd.read_excel("./data.xlsx")
#
#ans = df.loc[(df.SERIAL_NUMBER == "TBCAC2018136") & df.history.notnull()]
#ans = ans.astype({"history": int})
#
# print(ans)

class BreakIt(Exception):
    pass


for i in range(5):
    print(f"i={i}")
    try:
        for j in range(10):
            print(f"  j={j} -> ", end="", flush=True)
            for k in range(10):
                print(f"{k} ", end="", flush=True)
                if k == 4:
                    raise BreakIt
            print()
    except BreakIt:
        print()
        continue
