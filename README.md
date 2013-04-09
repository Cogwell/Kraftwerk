####################################################################################################
#                                                                                                  #
#  $$\   $$\                     $$$$$$\    $$\                                       $$\          #
#  $$ | $$  |                   $$  __$$\   $$ |                                      $$ |         #
#  $$ |$$  /  $$$$$$\  $$$$$$\  $$ /  \__|$$$$$$\   $$\  $$\  $$\  $$$$$$\   $$$$$$\  $$ |  $$\    #
#  $$$$$  /  $$  __$$\ \____$$\ $$$$\     \_$$  _|  $$ | $$ | $$ |$$  __$$\ $$  __$$\ $$ | $$  |   #
#  $$  $$<   $$ |  \__|$$$$$$$ |$$  _|      $$ |    $$ | $$ | $$ |$$$$$$$$ |$$ |  \__|$$$$$$  /    #
#  $$ |\$$\  $$ |     $$  __$$ |$$ |        $$ |$$\ $$ | $$ | $$ |$$   ____|$$ |      $$  _$$<     #
#  $$ | \$$\ $$ |     \$$$$$$$ |$$ |        \$$$$  |\$$$$$\$$$$  |\$$$$$$$\ $$ |      $$ | \$$\    #
#  \__|  \__|\__|      \_______|\__|         \____/  \_____\____/  \_______|\__|      \__|  \__|   #
#                                                                                                  #
####################################################################################################
#                                                                                                  #
#  Authers: Brian Clowers & Aaron Robinson       Company: Pacific Northwest National Laboratory    #
#  Last Update: 3/15/2012                                                                          #
#                                                                                                  #
####################################################################################################
# Kraftwerk is a python utility which assembles Yet Another Spectra Database Format (YASDBF) files #
# YASDBF files contain the modules used to stitch multiple mass spectrometry data formats into a   #
# SQLite database. YASDBF files are then extracted by Kraftwerk into pipelines.                    #
####################################################################################################
# folder heircharhy (/home/yasdbf/):
#   kraftwerk.py -> the core code for building the flexibly YASDBF format and data pipeline.
#   build.sh -> this bash script is an example of how to use kraftwerk to build a yasdbf file.
#   template.yasdbf -> this is a basic outline for creating a YASDBF for a general pipeline.
#     -> apps
#     -> ontology
#     -> setup_info
#     -> source
####################################################################################################
