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
import xlrd

emtab_data = xlrd.open_workbook('emtab.xlsx')
hmr_data = xlrd.open_workbook('hmr.xlsx')

emtab_table = emtab_data.sheet_by_index(0)
hmr_table = hmr_data.sheet_by_index(0)

emtab_rows = emtab_table.nrows
hmr_rows = hmr_table.nrows

print 'emtab rows:', emtab_rows
print 'hmr_rows:', hmr_rows

emtab_colnames = emtab_table.row_values(3)
print emtab_colnames
conn = _sqlite3.connect('bio.rdb')
c = conn.cursor()
c.execute('delete from e_mtab')
c.execute('delete from HMR')

emtab_insert_sql = '''
insert into e_mtab( 
Gene_ID, 
Gene_Name, 
EBV_transformed_lymphocyte, 
adrenal_gland, 
amygdala, 
animal_ovary, 
anterior_cingulate_cortex_BA24_of_brain, 
aorta, 
atrial_appendage_of_heart, 
breast_mammary_tissue, 
caudate_basal_ganglia_of_brain, 
cerebellar_hemisphere_of_brain, 
cerebellum, 
cerebral_cortex, 
coronary_artery, 
cortex_of_kidney, 
ectocervix, 
esophagus_muscularis_mucosa, 
fallopian_tube, 
frontal_cortex_BA9, 
gastroesophageal_junction, 
heart_left_ventricle, 
hippocampus, 
hypothalamus, 
leukemia_cell_line, 
liver, 
lung, 
minor_salivary_gland, 
mucosa_of_esophagus, 
nucleus_accumbens_basal_ganglia, 
pancreas, 
pituitary_gland, 
prostate_gland, 
putamen_basal_ganglia, 
sigmoid_colon, 
skeletal_muscle, 
skin_of_lower_leg, 
skin_of_suprapubic_region, 
spinal_cord_cervical_c_1, 
spleen, 
stomach, 
subcutaneous_adipose_tissue, 
substantia_nigra, 
terminal_ileum_of_small_intestine, 
testis, 
thyroid, 
tibial_artery, 
tibial_nerve, 
transformed_fibroblast, 
transverse_colon, 
urinary_bladder, 
uterus, 
vagina, 
visceral_omentum_adipose_tissue, 
whole_blood 
) values( 
?, 
?, 
?, 
?, 
?, 
?, 
?, 
?, 
?, 
?, 
?, 
?, 
?, 
?, 
?, 
?, 
?, 
?, 
?, 
?, 
?, 
?, 
?, 
?, 
?, 
?, 
?, 
?, 
?, 
?, 
?, 
?, 
?, 
?, 
?, 
?, 
?, 
?, 
?, 
?, 
?, 
?, 
?, 
?, 
?, 
?, 
?, 
?, 
?, 
?, 
?, 
?, 
?, 
?, 
? 
)
'''
hmr_insert_sql = "insert into HMR(rxnid,gene_association) values(?,?)"

for rownum in range(4, emtab_rows):
    row = emtab_table.row_values(rownum)
    c.execute(emtab_insert_sql, row)
for rownum in range(1, hmr_rows):
    row = hmr_table.row_values(rownum)
    if row is not "":
        c.execute(hmr_insert_sql, (row[1], row[5]))
conn.commit()
conn.close()
