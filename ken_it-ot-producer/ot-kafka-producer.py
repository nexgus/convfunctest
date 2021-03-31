#!/usr/bin/env python3.9

###########################################
#
# author: Kenduest Lee (kenduest@brobridge.com)
#
# Copyright by brobridge
#
###########################################

"""
usage: ot-kafka-producer.py [-h] [--server server] [--topic-id topic] [--kafka-timeout ms] [--batch-delay sec] [--data-each-delay sec]
                            [--data-start-time YYYY-mm-dd H:M:S[.ss]] [--batch-amount value] [--period-time sec] [--show-every value]
                            [--verbose-error]

optional arguments:
  -h, --help            show this help message and exit

General server options:
  --server server       kafka server, use "," for multi (default: 10.1.5.42,10.1.5.43,10.1.5.44)

  --topic-id topic      kafka topic name (default: ot_benchmark_test)

  --kafka-timeout ms    kafka server timeout. unit: ms (default: 30000 ms)

Data time options:
  --batch-delay sec     delay time for every batch (0: for unlimited, default is 0)
                        每跑產生 13 筆資料之後，要 delay 多久時間再繼續產生數據資料

  --data-each-delay sec
                        seconds for generated timestamp between each data (0 for un-specified, default is 10)
                        每跑產生 13 筆資料之後, 下一批資料時間間隔誤差時間

  --data-start-time YYYY-mm-dd H:M:S[.ss]
                        data start time for timestamp (ex: 2021-03-24 02:32:25.718814, default is now)
                        指定資料起始時間

Data handle options:
  --batch-amount value  total batch amount (0 for unlimited, default is 10000)
                        資料跑批次要進行幾次

  --period-time sec     run within period seconds (0 for unlimited, default is 600)
                        指定程式跑多久時間就結束

  --show-every value    show progress every amount (0 for disabled, default is 0)
                        跑幾次批次就產生目前進度

Other options:
  --verbose-error       set show verbose error

"""

import asyncio
import copy
import datetime
import json
import random
import sys
from argparse import ArgumentParser

try:
    from aiokafka import AIOKafkaProducer
except:
    sys.stderr.write("\nInfo: pip install aiokafka first\n")


class DefaultSettings:
    SERVER = "10.1.5.42,10.1.5.43,10.1.5.44"
    TOPIC_ID = "ot_benchmark_test"
    GROUP_ID = None

    BATCH_AMOUNT = 10000
    PERIOD = 600
    DELAY_SECONDS = 0
    PROGRESS_AMOUNT = 0
    KAFKA_TIMEOUT = 30000

    BETWEEN_EACH_DELAY = 10


data_template1 = [
    {"SMT_A_3_detect_num": 108, "SMT_A_3_OK_num": 96, "SMT_A_3_OK_rate": 88.888,
     "SMT_A_3_NG_num": 0, "SMT_A_3_NG_rate": 0, "SMT_A_3_manual_OK_num": 12, "SMT_A_3_manual_OK_rate": 11.111,
     "SMT_A_3_total_OK_num": 108, "SMT_A_3_total_OK_rate": 100, "SMT_A_3_total_SN": None},

    {"DIP_A_1_productName": "onoros", "DIP_A_1_left_workpiece": "roM",
     "DIP_A_1_right_workpiece": "0N01505", "DIP_A_1_total_num": 195, "DIP_A_1_cutting_time": ".",
     "DIP_A_1_cutting_accu_dist": ".", "DIP_A_1_cutting_allow_dist": ".", "DIP_A_1_auto_feed_count": None,
     "DIP_A_1_spindle_time": 881},

    {"SMT_B_2_base_num": 390, "SMT_B_2_op_rate": 23.55, "SMT_B_2_cycleTime": "0:00:35.20",
     "SMT_B_2_totalTime": "0:00:51.50", "SMT_B_2_total_num": 42.6, "SMT_B_2_print_num": 390,
     "SMT_B_2_cleanCount_1st": 2, "SMT_B_2_cleanCount_2nd": None, "SMT_B_2_solder_count": 4,
     "SMT_B_2_production_status": "工序等待中", "SMT_B_2_WO": None, "SMT_B_2_status": None,
     "SMT_B_2_program_number": "4BB00XCCB2X10-B"},

    {"SMT_A_2_PCB_name": "-320-0", "SMT_A_2_cleanMode_1": 3, "SMT_A_2_cleanMode_2": 0,
     "SMT_A_2_count": 28, "SMT_A_2_batch_num": 108, "SMT_A_2_limit": 0, "SMT_A_2_cycleTime": 22.5,
     "SMT_A_2_pressure_1": 0, "SMT_A_2_pressure_2": 0, "SMT_A_2_speed_1": 35, "SMT_A_2_speed_2": 35,
     "SMT_A_2_limit_1": 0, "SMT_A_2_limit_2": 0, "SMT_A_2_releasing_speed": 1.5, "SMT_A_2_releasing_distance": 3,
     "SMT_A_2_print_speed": 0, "SMT_A_2_clean_rate_1": 3, "SMT_A_2_clean_rate_2": 0, "SMT_A_2_forward_x": -0.02,
     "SMT_A_2_backward_x": -0.02, "SMT_A_2_forward_y": -0.02, "SMT_A_2_backward_y": -0.02, "SMT_A_2_forward_theta": 0,
     "SMT_A_2_backward_theta": 0},

    {"DIP_A_5_PCB_name": "NGS13SWOB", "DIP_A_5_mode": "AUTO", "DIP_A_5_machine_status": "RUN",
     "DIP_A_5_accu_count": 503596, "DIP_A_5_count": 421308, "DIP_A_5_WO": "H13-20120119", "DIP_A_5_type": "NID12GGDIP",
     "DIP_A_5_SN": "27Q000K6B0SX1", "DIP_A_5_OK_count": 9, "DIP_A_5_total_count": 100, "DIP_A_5_last_WO": "H16-2012011",
     "DIP_A_5_last_type": "NCS12GG", "DIP_A_5_last_SN": "20Q000K6B0SX1", "DIP_A_5_last_WO_version": "30KE",
     "DIP_A_5_last_BIOS": "Z135004", "DIP_A_5_last_CKS": 205, "DIP_A_5_status": None},

    {"DIP_A_4_cycle_count": 35155, "DIP_A_4_count": 5, "DIP_A_4_cycleTime": "0:00:00.0",
     "DIP_A_4_tin_temp": 290, "DIP_A_4_tin_speek": 0, "DIP_A_4_air_pressure": 54, "DIP_A_4_bottle_pressure": 5,
     "DIP_A_4_N2_pressure": 53},

    {"DIP_A_3_SN_1": "ACA70A020100032110", "DIP_A_3_time_1": 31.82, "DIP_A_3_result_1": "不良",
     "DIP_A_3_SN_2": "A7A70A020100032110", "DIP_A_3_time_2": 31.57, "DIP_A_3_result_2": "不良",
     "DIP_A_3_SN_3": "ACA70A02010003", "DIP_A_3_time_3": 32.15, "DIP_A_3_result_3": "不良",
     "DIP_A_3_program_number": "A6S136Rev:A"},

    {"SMT_B_4_N2": 1481, "SMT_B_4_Fan1": 60, "SMT_B_4_Fan2": 65, "SMT_B_4_Fan3": 70,
     "SMT_B_4_Fan4": 75, "SMT_B_4_Fan5": 45, "SMT_B_4_Fan6": 40, "SMT_B_4_speed": 65, "SMT_B_4_RCS": "ON"},

    {"DIP_A_2_MB_SN": "DNB1160-S--", "DIP_A_2_MB_count": "381PCS",
     "DIP_A_2_total_num": "1S54PCS", "DIP_A_2_N2_1_SV": "111kp", "DIP_A_2_N2_2_SV": "121kp", "DIP_A_2_N2_3_SV": "131kp",
     "DIP_A_2_N2_4_SV": "141kp", "DIP_A_2_preHeat_1_PV": 270, "DIP_A_2_preHeat_1_SV": 280, "DIP_A_2_preHeat_2_PV": 280,
     "DIP_A_2_preHeat_2_SV": 290, "DIP_A_2_preHeat_3_PV": 300, "DIP_A_2_preHeat_3_SV": 300,
     "DIP_A_2_top_preHeat_1_PV": 146, "DIP_A_2_top_preHeat_1_SV": 130, "DIP_A_2_top_preHeat_2_PV": 134,
     "DIP_A_2_top_preHeat_2_SV": "1MT", "DIP_A_2_tin_temp_PV": 270, "DIP_A_2_tin_temp_SV": 270,
     "DIP_A_2_tin_height_PV": 9, "DIP_A_2_tin_height_SV": 6, "DIP_A_2_peak_1_SV": 0, "DIP_A_2_peak_2_SV": 10,
     "DIP_A_2_rail_width_PV": 2281, "DIP_A_2_rail_width_SV": 280, "DIP_A_2_speed_PV": 850, "DIP_A_2_speed_SV": 850},

    {"SMT_B_1_WO": None, "SMT_B_1_last_SN": None, "SMT_B_1_SN": None, "SMT_B_1_cycleTime": None,
     "SMT_B_1_program": None, "SMT_B_1_auto": None, "SMT_B_1_run": None, "SMT_B_1_status": None,
     "SMT_B_1_current_SN": None, "SMT_B_1_power": None, "SMT_B_1_scan_rate": None},

    {"SMT_B_3_machine_name": "name", "SMT_B_3_SN": None, "SMT_B_3_program_number": None,
     "SMT_B_3_WO": None, "SMT_B_3_whole_OK": None, "SMT_B_3_whole_NG": None, "SMT_B_3_whole_reOK": None,
     "SMT_B_3_whole_yieldRate": None, "SMT_B_3_board_OK": None, "SMT_B_3_board_NG": None, "SMT_B_3_board_reOK": None,
     "SMT_B_3_board_yieldRate": None, "SMT_B_3_component_OK": None, "SMT_B_3_component_NG": None,
     "SMT_B_3_component_reOK": None, "SMT_B_3_component_yieldRate": None, "SMT_B_3_tin_OK": None,
     "SMT_B_3_tin_NG": None, "SMT_B_3_tin_reOK": None, "SMT_B_3_tin_yieldRate": None},

    {"SMT_A_1_WO": None, "SMT_A_1_last_SN": None, "SMT_A_1_SN": None, "SMT_A_1_cycleTime": None,
     "SMT_A_1_program": None, "SMT_A_1_auto": None, "SMT_A_1_run": None, "SMT_A_1_status": None,
     "SMT_A_1_current_SN": None, "SMT_A_1_power": None, "SMT_A_1_scan_rate": None},

    {"TI_SMT001": 25.639999389648438, "HI_SMT001": 53.7599983215332,
     "CO2_SMT001": 479, "PM_SMT001": 20, "TI_DIP001": 25.770000457763672, "HI_DIP001": 57.560001373291016,
     "CO2_DIP001": 493, "PM_DIP001": 5, "TI_SYS001": 24.84000015258789, "HI_SYS001": 59.91999816894531,
     "CO2_SYS001": 505, "PM_SYS001": 4, "TI_IQC001": 25.200000762939453, "HI_IQC001": 58.41999816894531,
     "CO2_IQC001": 493, "PM_IQC001": 7, "TI_WAR001": 25.040000915527344, "HI_WAR001": 60.7599983215332,
     "CO2_WAR001": 510, "PM_WAR001": 7, "TI_OFF001": 25.229999542236328, "HI_OFF001": 59, "CO2_OFF001": 645,
     "PM_OFF001": 11, "TI_E001": 22.860000610351562, "HI_E001": 65.02999877929688,
     "TI_E002": 23.40999984741211, "HI_E002": 61.33000183105469, "TI_E003": 23.34000015258789,
     "HI_E003": 62.959999084472656, "TI_E004": 23.219999313354492, "HI_E004": 60.31999969482422,
     "TI_E005": 23.799999237060547, "HI_E005": 58.88999938964844, "TI_E006": 22.65999984741211,
     "HI_E006": 64.1500015258789, "TI_E007": 22.520000457763672, "HI_E007": 61.939998626708984,
     "TI_E008": 22.65999984741211, "HI_E008": 65.70999908447266, "TI_E009": 22.110000610351562,
     "HI_E009": 63.970001220703125, "TI_E011": 21.360000610351562, "HI_E011": 61.97999954223633,
     "TI_E012": 22.8700008392334, "HI_E012": 60.2599983215332, "TI_E013": 22.610000610351562,
     "HI_E013": 64.16000366210938, "TI_E014": 23.950000762939453, "HI_E014": 62.45000076293945,
     "TI_E015": 23.559999465942383, "HI_E015": 64.62999725341797, "TI_E016": 23.8799991607666,
     "HI_E016": 62.15999984741211, "TI_E017": 23.290000915527344, "HI_E017": 63.66999816894531,
     "TI_E018": 24.06999969482422, "HI_E018": 61.58000183105469, "TI_E019": 32.939998626708984,
     "HI_E019": 32.18000030517578, "TI_E020": 35.29999923706055, "HI_E020": 31, "TI_E021": 35.599998474121094,
     "HI_E021": 31, "TI_E022": 34.79999923706055, "HI_E022": 31, "TI_E023": 34.5, "HI_E023": 33,
     "TI_E024": 35.20000076293945, "TI_E025": 36.20000076293945, "TI_E026": 35.099998474121094,
     "TI_E027": 36.900001525878906, "TI_E028": 37.900001525878906, "TI_D001": 126, "TI_D002": 60,
     "TI_D003": 110, "TI_D004": 79, "PT_D004": 7.199999809265137, "TI_D005": 36, "PT_D005": 7.199999809265137,
     "PT_D006": 6.900000095367432, "TI_D006": -29.899999618530273, "FI_D007": 0, "TI_D008": 26.299999237060547,
     "HI_D008": 0.5, "TI_D009": 25.200000762939453, "HI_D009": 23.700000762939453,
     "TI_D010": 24.100000381469727, "HI_D010": 47.79999923706055, "TI_D011": 3.8183839321136475,
     "FI_N2007": 129421, "FI_OFFCW012": 14.372880935668945, "TI_INCW012": 13.093704223632812,
     "TI_OUTCW012": 11.169578552246094, "FI_ACCCW012": 454.0811462402344, "TI_INCW013": 11.800000190734863,
     "TI_OUTCW013": 14.300000190734863, "FI_DIPCW013": 1.891752926838503e-43, "FI_ACCCW013": 3046578,
     "FI_KWCW013": 407.9496765136719, "PWM_B001_KW": 41.991912841796875, "PWM_B001_PF": 94.69729614257812,
     "PWM_B001_KWH": 557207.375, "PWM_B001_KWH_A": 40329.8984375, "PWM_B001_KWH_B": 252807.171875,
     "PWM_B001_KWH_C": 221202.203125, "PWM_B001_KWH_A_yest": 40329.8984375,
     "PWM_B001_KWH_B_yest": 252672.21875, "PWM_B001_KWH_C_yest": 221064.921875,
     "PWM_B002_KW": 8.336069107055664, "PWM_B002_PF": -75.71380615234375, "PWM_B002_KWH": 131991.375,
     "PWM_B002_KWH_A": 9039.1865234375, "PWM_B002_KWH_B": 57216.51171875, "PWM_B002_KWH_C": 55257.7890625,
     "PWM_B002_KWH_A_yest": 9039.1865234375, "PWM_B002_KWH_B_yest": 57188.77734375,
     "PWM_B002_KWH_C_yest": 55222.46484375, "PWM_B003_KW": 0, "PWM_B003_PF": 0,
     "PWM_B003_KWH": 2807.025634765625, "PWM_B003_KWH_A": 531.89306640625, "PWM_B003_KWH_B": 1632.667236328125,
     "PWM_B003_KWH_C": 529.7237548828125, "PWM_B003_KWH_A_yest": 531.89306640625,
     "PWM_B003_KWH_B_yest": 1631.601318359375, "PWM_B003_KWH_C_yest": 529.3648071289062,
     "PWM_B004_KW": 189.16172790527344, "PWM_B004_PF": 96.69367218017578, "PWM_B004_KWH": 2740980.5,
     "PWM_B004_KWH_A": 260582.421875, "PWM_B004_KWH_B": 1393194.25, "PWM_B004_KWH_C": 925111.375,
     "PWM_B004_KWH_A_yest": 260582.421875, "PWM_B004_KWH_B_yest": 1392336.5,
     "PWM_B004_KWH_C_yest": 924290.1875, "PWM_CW005_KW": 20.665143966674805, "PWM_CW005_PF": 84.32579040527344,
     "PWM_CW005_KWH": 397471.375, "PWM_CW005_KWH_A": 53867.30859375, "PWM_CW005_KWH_B": 201018.234375,
     "PWM_CW005_KWH_C": 120081.34375, "PWM_CW005_KWH_A_yest": 53867.30859375,
     "PWM_CW005_KWH_B_yest": 200908.9375, "PWM_CW005_KWH_C_yest": 119925.9453125,
     "PWM_CW006_KW": 82.50379943847656, "PWM_CW006_PF": 89.71480560302734, "PWM_CW006_KWH": 942524,
     "PWM_CW006_KWH_A": 90253.5546875, "PWM_CW006_KWH_B": 477412.5, "PWM_CW006_KWH_C": 324562.65625,
     "PWM_CW006_KWH_A_yest": 90253.5546875, "PWM_CW006_KWH_B_yest": 477016.96875,
     "PWM_CW006_KWH_C_yest": 324013.5, "PWM_CW007_KW": 0, "PWM_CW007_PF": 0, "PWM_CW007_KWH": 0,
     "PWM_CW007_KWH_A": 0, "PWM_CW007_KWH_B": 0, "PWM_CW007_KWH_C": 0, "PWM_CW007_KWH_A_yest": 0,
     "PWM_CW007_KWH_B_yest": 0, "PWM_CW007_KWH_C_yest": 0, "PWM_SMT008_V": 227.97999572753906,
     "PWM_SMT008_A": 49.347999572753906, "PWM_SMT008_PF": 76.4000015258789,
     "PWM_SMT008_KW": 26.883800506591797, "PWM_SMT008_KWH": 407127.59375, "PWM_SMT009_V": 228.41000366210938,
     "PWM_SMT009_A": 28.40999984741211, "PWM_SMT009_PF": 99.80000305175781,
     "PWM_SMT009_KW": 10.854599952697754, "PWM_SMT009_KWH": 250558.265625, "PWM_SMT010_V": 228.63999938964844,
     "PWM_SMT010_A": 43.874000549316406, "PWM_SMT010_PF": 75.30000305175781,
     "PWM_SMT010_KW": 22.708900451660156, "PWM_SMT010_KWH": 314278.84375, "PWM_DIP011_V": 229.61000061035156,
     "PWM_DIP011_A": 55.78900146484375, "PWM_DIP011_PF": 98.30000305175781,
     "PWM_DIP011_KW": 29.747299194335938, "PWM_DIP011_KWH": 743064.5, "PWM_SYS012_V": 128.5399932861328,
     "PWM_SYS012_A": 28.434999465942383, "PWM_SYS012_PF": -71.19999694824219,
     "PWM_SYS012_KW": 7.8196001052856445, "PWM_SYS012_KWH": 119959.078125, "PWM_SYS013_V": 223.60000610351562,
     "PWM_SYS013_A": 20.08099937438965, "PWM_SYS013_PF": 87.0999984741211, "PWM_SYS013_KW": 5.031499862670898,
     "PWM_SYS013_KWH": 269362.28125, "PWM_SYS014_V": 224.0399932861328, "PWM_SYS014_A": 7.265999794006348,
     "PWM_SYS014_PF": -82.19999694824219, "PWM_SYS014_KW": -2.593100070953369, "PWM_SYS014_KWH": 227995.9375,
     "PWM_SYS015_V": 224, "PWM_SYS015_A": 0, "PWM_SYS015_PF": 0, "PWM_SYS015_KW": 0,
     "PWM_SYS015_KWH": 3714.695068359375, "PWM_OFF016_V": 228.9199981689453, "PWM_OFF016_A": 16.29199981689453,
     "PWM_OFF016_PF": 99.9000015258789, "PWM_OFF016_KW": 4.448699951171875, "PWM_OFF016_KWH": 99131.890625,
     "LED_DRY_SMT02": 2, "LED_N2M_SMT02": 2, "PR1_N2M_SMT02": 5.199999809265137,
     "PR2_N2M_SMT02": 7.599999904632568, "O2_N2M_SMT02": 82, "LED_ACP_SMT02": 2,
     "TI_ACP_SMT02": 61.80555725097656, "LED_LSR_SMT01": 1, "UPT_LSR_SMT01": 50, "LED_LSR_SMT02": 1,
     "UPT_LSR_SMT02": 50, "LED_SPT_SMT01": 2, "UPT_SPT_SMT01": 100, "LED_SPT_SMT02": 1, "UPT_SPT_SMT02": 50,
     "LED_DSP_SMT02": 1, "UPT_DSP_SMT02": 50, "LED_CML01": 2, "UPT_CML01": -1, "PUI_CML01": -1,
     "LBN_CML01": -1, "LED_CML02": 0, "UPT_CML02": -1, "PUI_CML02": -1, "LBN_CML02": -1, "LED_NPM01": 2,
     "UPT_NPM01": -1, "PUI_NPM01": -1, "LBN_NPM01": -1, "LED_NPM02": 0, "UPT_NPM02": -1, "PUI_NPM02": -1,
     "LBN_NPM02": -1, "LED_NPM03": 2, "UPT_NPM03": -1, "PUI_NPM03": -1, "LBN_NPM03": -1, "LED_NPM04": 2,
     "UPT_NPM04": -1, "PUI_NPM04": -1, "LBN_NPM04": -1, "LED_RFL01": 2, "TMP01_RFL01": 250.34921264648438,
     "TMP02_RFL01": 257.5722351074219, "TMP03_RFL01": 254.21908569335938, "TMP04_RFL01": 254.54360961914062,
     "TMP05_RFL01": 257.15069580078125, "TMP06_RFL01": 250.62110900878906, "TMP07_RFL01": 254.5689697265625,
     "TMP08_RFL01": 250.4629669189453, "TMP09_RFL01": 256.8896484375, "TMP10_RFL01": 253.73931884765625,
     "TMP11_RFL01": 254.40968322753906, "TMP12_RFL01": 258.5874328613281, "LED_RFL02": 2,
     "TMP01_RFL02": 258.6451721191406, "TMP02_RFL02": 259.302001953125, "TMP03_RFL02": 254.53428649902344,
     "TMP04_RFL02": 251.30055236816406, "TMP05_RFL02": 250.2776336669922, "TMP06_RFL02": 258.33770751953125,
     "TMP07_RFL02": 257.89520263671875, "TMP08_RFL02": 253.34642028808594, "TMP09_RFL02": 258.2105712890625,
     "TMP10_RFL02": 255.71620178222656, "TMP11_RFL02": 258.9808349609375, "TMP12_RFL02": 252.30801391601562,
     "LED_AOI01": 1, "YRT_AOI01": 100, "LED_AOI02": 1, "YRT_AOI02": 100, "LED_NPM05": 0, "UPT_NPM05": 100,
     "PUI_NPM05": 0.01737525872886181, "LED_VAC01": 2, "UPT_VAC01": 90.48438262939453, "LED_IRM01": 2,
     "UPT_IRM01": 100, "LED_SPR01": 2, "UPT_SPR01": 99.26063537597656, "LED_WAV01": 2, "TMP_WAV01": 450,
     "LED_AOI03": 1, "YRT_AOI03": 50, "LED_MSM01": 2, "TMP_MSM01": 290, "LED_LAB01": 2, "UPT_LAB01": 100,
     "LED_SPI01": 1, "YRT_SPI01": 50, "LED_SPI02": 1, "YRT_SPI02": 50,
     "PDM_CompA_MH_Overall": 0.15573285520076752, "PDM_CompA_MH_Band1": 0.1662529557943344,
     "PDM_CompA_MH_Band2": 0.009928354993462563, "PDM_CompA_MH_Band3": 0.009420016780495644,
     "PDM_CompA_MH_Band4": 0.00597013533115387, "PDM_CompA_SH_Overall": 0.1436578780412674,
     "PDM_CompA_SH_Band1": 0.1711207777261734, "PDM_CompA_SH_Band2": 0.011639195494353771,
     "PDM_CompA_SH_Band3": 0.010808464139699936, "PDM_CompA_SH_Band4": 0.005539086647331715,
     "PDM_ScruberFanA_MH_Overall": 0.14746683835983276, "PDM_ScruberFanA_MH_Band1": 0.0489858016371727,
     "PDM_ScruberFanA_MH_Band2": 0.012352905236184597, "PDM_ScruberFanA_MH_Band3": 0.05169980973005295,
     "PDM_ScruberFanA_MH_Band4": 0.026463013142347336, "PDM_ScruberFanA_FH_Overall": 0.15792976319789886,
     "PDM_ScruberFanA_FH_Band1": 0.041834697127342224, "PDM_ScruberFanA_FH_Band2": 0.008542215451598167,
     "PDM_ScruberFanA_FH_Band3": 0.0705382227897644, "PDM_ScruberFanA_FH_Band4": 0.03414194658398628,
     "PDM_CompB_MH_Overall": 1.0032944679260254, "PDM_CompB_MH_Band1": 0.6591511368751526,
     "PDM_CompB_MH_Band2": 0.4134596884250641, "PDM_CompB_MH_Band3": 0.2725280821323395,
     "PDM_CompB_MH_Band4": 0.23855501413345337, "PDM_CompB_SH_Overall": 1.1725475788116455,
     "PDM_CompB_SH_Band1": 1.0990333557128906, "PDM_CompB_SH_Band2": 0.2584807276725769,
     "PDM_CompB_SH_Band3": 0.472839891910553, "PDM_CompB_SH_Band4": 0.04454473406076431,
     "PDM_ScruberFanB_MH_Overall": 1.5389575958251953, "PDM_ScruberFanB_MH_Band1": 0.5681243538856506,
     "PDM_ScruberFanB_MH_Band2": 0.23180943727493286, "PDM_ScruberFanB_MH_Band3": 1.0135349035263062,
     "PDM_ScruberFanB_MH_Band4": 0.31130003929138184, "PDM_ScruberFanB_FH_Overall": 1.1510305404663086,
     "PDM_ScruberFanB_FH_Band1": 0.6651390790939331, "PDM_ScruberFanB_FH_Band2": 0.1875099092721939,
     "PDM_ScruberFanB_FH_Band3": 0.49908924102783203, "PDM_ScruberFanB_FH_Band4": 0.1635548621416092,
     "PDM_DIP_M1H_Overall": 0.3387365937232971, "PDM_DIP_M1H_Band1": 0.15391886234283447,
     "PDM_DIP_M1H_Band2": 0.22202110290527344, "PDM_DIP_M1H_Band3": 0.009404676966369152,
     "PDM_DIP_M1H_Band4": 0.010082057677209377, "PDM_DIP_M2H_Overall": 0.5247890949249268,
     "PDM_DIP_M2H_Band1": 0.5235911011695862, "PDM_DIP_M2H_Band2": 0.18494918942451477,
     "PDM_DIP_M2H_Band3": 0.05567585304379463, "PDM_DIP_M2H_Band4": 0.01988275721669197,
     "PDM_REFLOW_RAMH_Overall": 1.0625518560409546, "PDM_REFLOW_RAMH_Band1": 0.8350433707237244,
     "PDM_REFLOW_RAMH_Band2": 0.25848186016082764, "PDM_REFLOW_RAMH_Band3": 0.8305565714836121,
     "PDM_REFLOW_RAMH_Band4": 0.4674381613731384, "PDM_REFLOW_RBMH_Overall": 0.46149343252182007,
     "PDM_REFLOW_RBMH_Band1": 0.4026397168636322, "PDM_REFLOW_RBMH_Band2": 0.11424074321985245,
     "PDM_REFLOW_RBMH_Band3": 0.2506270110607147, "PDM_REFLOW_RBMH_Band4": 0.1315554678440094
     }
]


def compose_random_data(timestamp=None, inc=random.uniform(0.001, 0.009)):
    data = copy.copy(data_template1)

    number_random_list = [n for n in range(0, 13)]
    random.shuffle(number_random_list)

    result = []

    t = timestamp if timestamp else datetime.datetime.now().timestamp()

    for i in number_random_list:
        d = data[i]

        d["Time"] = t
        result.append(d)
        t += inc

    return result


async def send_one(args):
    server_name = args.server
    topic_id = args.topic_id
    batch_amount = args.batch_amount
    period_time = args.period_time
    show_every = args.show_every
    delay_time = args.delay_time
    server_timeout = args.server_timeout
    data_start_time = args.data_start_time
    between_each_delay = args.between_each_delay

    current_amount = 0

    sys.stdout.write("\n")

    if server_timeout < 0:
        server_timeout = DefaultSettings.KAFKA_TIMEOUT

    try:
        if data_start_time == "":
            data_start_time = datetime.datetime.now().timestamp()
        else:
            try:
                data_start_time = datetime.datetime.strptime(data_start_time, "%Y-%m-%d %H:%M:%S.%f").timestamp()
            except:
                data_start_time = datetime.datetime.strptime(data_start_time, "%Y-%m-%d %H:%M:%S").timestamp()
    except:
        sys.stderr.write("Error: Invalid time: %s\n\n" % args.data_start_time)
        sys.exit(1)

    producer = AIOKafkaProducer(bootstrap_servers=server_name, request_timeout_ms=server_timeout)

    start_timestamp = datetime.datetime.now().timestamp()
    used_time = 0

    try:

        await producer.start()

        while True:

            for data in compose_random_data(data_start_time):
                json_byte_data = json.dumps(data).encode()
                await producer.send(topic_id, json_byte_data)

            if delay_time > 0:
                await asyncio.sleep(delay_time)

            current_amount += 1
            data_start_time += between_each_delay

            used_time = datetime.datetime.now().timestamp() - start_timestamp

            if period_time > 0 and used_time >= period_time:
                break

            if show_every > 0 and current_amount % show_every == 0:
                if batch_amount == 0:
                    sys.stdout.write("Current Progress amount: %d\r" % (current_amount,))
                    sys.stdout.flush()
                else:
                    sys.stdout.write("Current Progress amount: %d/%d\r" % (current_amount, batch_amount))
                    sys.stdout.flush()
            if batch_amount > 0 and current_amount >= batch_amount:
                break

    finally:
        await producer.flush()
        await producer.stop()

    if show_every:
        if batch_amount == 0:
            sys.stdout.write("\rCurrent Progress amount: %d/%s\r\n\n" % (current_amount, batch_amount))
        else:
            sys.stdout.write("\rCurrent Progress amount: %d\r\n\n" % current_amount)
            sys.stdout.write("Total required batch amount: %s\n" % batch_amount)
    sys.stdout.write("Total completed batch amount: %d\n" % current_amount)
    sys.stdout.write("Total progress used seconds: %.6f\n\n" % used_time)
    sys.stdout.flush()


def handle_check_args(args):
    if args.between_each_delay < 0:
        sys.stderr.write("\nError: incorrect delay time: %s\n\n" % args.between_each_delay)
        sys.exit(1)

    if args.batch_amount < 0:
        sys.stderr.write("\nError: incorrect batch amount: %s\n\n" % args.batch_amount)
        sys.exit(1)

    if args.show_every < 0:
        sys.stderr.write("\nError: incorrect show progress second: %s\n\n" % args.show_every)
        sys.exit(1)


def handle_kafka(args):
    handle_check_args(args)

    sys.stdout.write("\n{x} Envionment {x}\n\n".format(x="*" * 10))
    sys.stdout.write("1. Using Kakfa: %s\n" % ", ".join(args.server.split(",")))
    sys.stdout.write("1. Using Topic: %s\n" % args.topic_id)
    sys.stdout.write(
        "2. Program run within Time: %s (seconds)\n" % (args.period_time if args.period_time > 0 else "N/A"))
    sys.stdout.write("2. Data Timestamp Start: %s\n" % args.data_start_time)
    sys.stdout.write("2. Data lag every batch: %f (seconds)\n" % args.between_each_delay)
    sys.stdout.write("3. Batch Amount: %s \n" % (args.batch_amount if args.batch_amount > 0 else "N/A"))

    sys.stdout.flush()

    try:
        asyncio.run(send_one(args))
    except (asyncio.CancelledError, KeyboardInterrupt):
        sys.exit(0)


def main():
    parser = ArgumentParser()

    group1 = parser.add_argument_group('General server options')

    group1.add_argument(
        "--server",
        help="kafka server, use \",\" for multi (default: %s)" % DefaultSettings.SERVER,
        required=False,
        dest="server",
        default=DefaultSettings.SERVER,
        metavar="server"
    )

    group1.add_argument(
        "--topic-id",
        help="kafka topic name (default: %s)" % DefaultSettings.TOPIC_ID,
        required=False,
        dest="topic_id",
        default=DefaultSettings.TOPIC_ID,
        metavar="topic",
    )

    group1.add_argument(
        "--kafka-timeout",
        help="kafka server timeout. unit: ms (default: %s ms)" % DefaultSettings.KAFKA_TIMEOUT,
        required=False,
        dest="server_timeout",
        default=DefaultSettings.KAFKA_TIMEOUT,
        metavar="ms",
        type=float,
    )

    group2 = parser.add_argument_group('Data time options')

    group2.add_argument(
        "--batch-delay",
        help="delay time for every batch (0: for unlimited, default is %s)" % DefaultSettings.DELAY_SECONDS,
        type=float,
        required=False,
        dest="delay_time",
        default=DefaultSettings.DELAY_SECONDS,
        metavar="sec"
    )

    group2.add_argument(
        "--data-each-delay",
        help="data lag time for generated timestamp between each data (0 for un-specified, default is %d)" % DefaultSettings.BETWEEN_EACH_DELAY,
        required=False,
        type=float,
        dest="between_each_delay",
        default=DefaultSettings.BETWEEN_EACH_DELAY,
        metavar="sec",
    )

    group2.add_argument(
        "--data-start-time",
        help="data start time for timestamp (ex: %s, default is now)" % datetime.datetime.now(),
        required=False,
        type=str,
        dest="data_start_time",
        default=str(datetime.datetime.now()),
        metavar="YYYY-mm-dd H:M:S[.ss]",
    )

    group3 = parser.add_argument_group('Data handle options')

    group3.add_argument(
        "--batch-amount",
        help="total batch amount (0 for unlimited, default is %s)" % DefaultSettings.BATCH_AMOUNT,
        type=int,
        required=False,
        dest="batch_amount",
        default=DefaultSettings.BATCH_AMOUNT,
        metavar="value"
    )

    group3.add_argument(
        "--period-time",
        help="run within period seconds (0 for unlimited, default is %s)" % DefaultSettings.PERIOD,
        required=False,
        type=float,
        dest="period_time",
        default=DefaultSettings.PERIOD,
        metavar="sec",
    )

    group3.add_argument(
        "--show-every",
        help="show progress every amount (0 for disabled, default is %s)" % DefaultSettings.PROGRESS_AMOUNT,
        required=False,
        type=int,
        dest="show_every",
        default=DefaultSettings.PROGRESS_AMOUNT,
        metavar="value",
    )

    group4 = parser.add_argument_group('Other options')

    group4.add_argument(
        "--verbose-error",
        help="set show verbose error",
        required=False,
        default=False,
        action="store_true",
    )

    args = parser.parse_args()

    try:
        handle_kafka(args)
    except Exception as e:
        if not args.verbose_error in (False, 0):
            sys.stderr.write("\n%s\n" % str(e))
        sys.stderr.write("\nQuit!\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
