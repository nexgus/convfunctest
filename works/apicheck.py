import pandas as pd
import requests
import time


class BreakIt(Exception):
    pass


class Default(object):
    host = "10.1.5.41"
    port = 80
    api = "/api/convergence/findRecord"
    excel = "./data.xlsx"


class Stats(object):
    def __init__(self, minimum=0, maximum=99999):
        self._min = maximum
        self._max = minimum
        self._avg = 0
        self._total = 0
        self._count = 0

    def average(self):
        return self._avg

    def count(self):
        return self._count

    def get(self):
        return {
            "count": self._count,
            "average": self._avg,
            "minimum": self._min,
            "maximum": self._max,
        }

    def maximum(self):
        return self._max

    def minimum(self):
        return self._min

    def reset(self):
        self.__init__()

    def set(self, val):
        self._count += 1
        if val < self._min:
            self._min = val
        if val > self._max:
            self._max = val
        self._total += val
        self._avg = self._total / self._count


def main(args):
    df = pd.read_excel(args.excel, sheet_name="Sheet1")
    all_sn = df.loc[df.history.notnull() & (df.ignore == False)
                    ].SERIAL_NUMBER.to_list()

    stats = Stats()
    total = len(all_sn)
    for idx, sn in enumerate(all_sn):
        # Get the answer
        ans = df.loc[df.history.notnull() & (df.SERIAL_NUMBER == sn)]
        ans = ans.astype({"history": int})
        print(f"[{idx+1}/{total}] {sn} -> ", end="", flush=True)

        # Generate URL for this serial number
        url = f"{Default.api}/{sn}"
        if url.startswith("/"):
            url = url[1:]
        url = f"http://{args.host}:{args.port}/{url}"

        # Send request
        t0 = time.time()
        resp = requests.get(url)
        t = time.time() - t0
        stats.set(t)

        t_avg = stats.average()
        sent = stats.count()
        print(f"{resp.status_code}, t={t:.6f} (avg={t_avg:.6f} @ {sent}) ",
              end="", flush=True)

        if not resp.ok:
            print()
            continue
        else:
            # Is body of resp jsonize?
            try:
                doc = json.loads(resp.content.decode("utf-8"))
            except Exception as ex:
                print(f"-> Failed to decode JSON")
                continue

            # Is serial_number ok?
            try:
                docsn = doc["serial_number"]
            except KeyError:
                print(f"-> Cannot find 'serial_number'")
                continue
            else:
                if sn != docsn:
                    print(
                        f"-> srial_number not match (expect {sn} but {docsn})")
                    continue

            # Now check history
            len_doc_history = len(doc["history"])
            len_history = ans.history.max() + 1
            if len_doc_history != len_history:
                print(
                    f"-> history length not match (expect {len_history} but {len_doc_history})")
                continue

            hidx = 0
            try:
                for doc_history in doc["history"]:
                    ans_history = ans.loc[ans.history == hidx]
                    idx = 0
                    for _, history in ans_history.iterrows():
                        if doc_history["id"] != history.it_id:
                            print(
                                f"-> history[{hidx}][{idx}].id not mismatch (expect {history.it_id} but {doc_history['id']})")
                            raise BreakIt
                        if doc_history["in_time"] != history.ts:
                            print(
                                f"-> history[{hidx}][{idx}].in_time not mismatch (expect {history.ts} but {doc_history['in_time']})")
                            raise BreakIt
                        measurements = list(
                            doc_history["measurements"].values())
                        if not all(measurements):
                            print(
                                f"-> the values in history[{hidx}][{idx}].measurements must be consistent")
                            raise BreakIt
                        if measurements[0] != history.ot_val:
                            print(
                                f"-> history[{hidx}][{idx}].measurements not match (expect {history.ot_val} but {measurements[0]})")
                            raise BreakIt

            except BreakIt:
                continue

            print("-> pass")

    print("-"*30)
    print(f"Request Sent:  {stats.count()}")
    print(f"Avg Resp Time: {stats.average():.6f}")
    print(f"Min Resp Time: {stats.minimum():.6f}")
    print(f"Max Resp Time: {stats.maximum():.6f}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--host", "-H", type=str, default=Default.host,
                        help="API server host address.")
    parser.add_argument("--port", "-p", type=int, default=Default.port,
                        help="API server port number.")
    parser.add_argument("--excel", "-x", type=str, default=Default.excel,
                        help="Path to Excel data file.")
    args = parser.parse_args()

    main(args)
