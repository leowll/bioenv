# -*- coding: utf-8 -*-
__author__ = 'leo'
"""
    package.module
    ~~~~~~~~~~~~~~

    A brief description goes here.

    :copyright: (c) YEAR by AUTHOR.
    :license: LICENSE_NAME, see LICENSE_FILE for more details.
"""

import _sqlite3
import threading
import csv

columns = [
    'EBV_transformed_lymphocyte',
    'adrenal_gland',
    'amygdala',
    'animal_ovary',
    'anterior_cingulate_cortex_BA24_of_brain',
    'aorta',
    'atrial_appendage_of_heart',
    'breast_mammary_tissue',
    'caudate_basal_ganglia_of_brain',
    'cerebellar_hemisphere_of_brain',
    'cerebellum',
    'cerebral_cortex',
    'coronary_artery',
    'cortex_of_kidney',
    'ectocervix',
    'esophagus_muscularis_mucosa',
    'fallopian_tube',
    'frontal_cortex_BA9',
    'gastroesophageal_junction',
    'heart_left_ventricle',
    'hippocampus',
    'hypothalamus',
    'leukemia_cell_line',
    'liver',
    'lung',
    'minor_salivary_gland',
    'mucosa_of_esophagus',
    'nucleus_accumbens_basal_ganglia',
    'pancreas',
    'pituitary_gland',
    'prostate_gland',
    'putamen_basal_ganglia',
    'sigmoid_colon',
    'skeletal_muscle',
    'skin_of_lower_leg',
    'skin_of_suprapubic_region',
    'spinal_cord_cervical_c_1',
    'spleen',
    'stomach',
    'subcutaneous_adipose_tissue',
    'substantia_nigra',
    'terminal_ileum_of_small_intestine',
    'testis',
    'thyroid',
    'tibial_artery',
    'tibial_nerve',
    'transformed_fibroblast',
    'transverse_colon',
    'urinary_bladder',
    'uterus',
    'vagina',
    'visceral_omentum_adipose_tissue',
    'whole_blood'
]
conn = _sqlite3.connect('bio.rdb')
c = conn.cursor()
c2 = conn.cursor()
c3 = conn.cursor()

e_mtab_select_sql = '''
  select adrenal_gland
  from e_mtab
  WHERE Gene_ID = '%s'

  '''
HMR_select_sql = '''
 select rxnid, gene_association from HMR
'''
c.execute(HMR_select_sql)
HMR_rows = c.fetchall()
final_result = []


class myThread(threading.Thread):  # 继承父类threading.Thread
    def __init__(self, col, HMR_rows, final_result):
        threading.Thread.__init__(self)
        self.col = col
        self.HMR_rows = HMR_rows
        self.final_result = final_result

    def run(self):  # 把要执行的代码写到run函数里面 线程在创建后会直接运行run函数
        populate_HMR_EMTAB(self.col, self.HMR_rows, self.final_result)


def write_csv(data_list, col_name):
    out_file = open(col_name, 'wb')
    wr = csv.writer(out_file)
    for d in data_list:
        wr.writerow(d)


def populate_HMR_EMTAB(col, HMR_rows, final_result):
    data_list = []
    conn = _sqlite3.connect('bio.rdb')
    c2 = conn.cursor()
    print col
    e_mtab_select_sql = '''
      select ''' + col + '''
      from e_mtab
      WHERE Gene_ID = '%s'
    '''

    result_list = []
    for row in HMR_rows:
        # print row[0]
        if row[1] <> u'':
            values = row[1].split(';')
            digit_values = []
            for v in values:
                c2.execute(e_mtab_select_sql % v)
                row2 = c2.fetchone()
                if row2 is not None:
                    for v2 in row2:
                        digit_values.append(v2)
            # print digit_values
            str_values = ";".join(str(e) for e in digit_values)
            # c3.execute(insert_sql, (row[0], str_values))
            # conn.commit()
            result_list.append([row[0], col, str_values])
        else:
            continue
    data_list.append(result_list)
    write_csv(data_list=data_list, col_name=col)


thread_list = []
for colume in columns:
    thread = myThread(colume, HMR_rows, final_result)
    thread_list.append(thread)

for t in thread_list:
    t.start()

for t in thread_list:
    t.join()

c.close()
