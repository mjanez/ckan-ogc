#!/usr/bin/env python3
## File: xlst_management.py
## Coding: UTF-8
## Author: Manuel Ángel Jáñez García (mjanez@tragsa.es)
## Institution: Tragsatec
## Project: EIKOS
## Goal: The goal of this script is is to map INSPIRE XML Metadata to GeoDCAT-AP.
## Parent: run.py 
""" Changelog:
    v1.0 - 8 Jul 2022: Create the first version
"""
# Update the version when apply changes
version = "1.0"

##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##
##          xlst_management.py          ##
##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##

## Import libraries   
from lxml.etree import XML, XSLT, tostring, fromstring
from urllib.request import urlopen
from xlst_config import config_getParameters
import os

# Config default path
config_file = "../config.yml"

def geodcatap_xml(xml, xsl):
    if xml.startswith("http://") or xml.startswith("https://"):
        with urlopen(xml) as response:
            xml_string = response.read()
        try:
            xml_name = output_folder + "/GeoDCAT-AP_" + inspire_info["xml_url"].split("&ID=")[-1] + ".xml"
        except:
            try:
                xml_name = output_folder + "/GeoDCAT-AP_" + inspire_info["xml_url"].split("/")[-1]
            except:
                xml_name = output_folder + "/GeoDCAT-AP_sample_" + inspire_info["xml_url"][-5] + ".xml"
    else:
        with open(xml, mode="rb") as xml_file:
            xml_string = xml_file.read()
        xml_name = os.path.basename(xml)

    if xsl.startswith("http://") or xsl.startswith("https://"):
        with urlopen(xsl) as response:
            xsl_string = response.read()
    else:
        with open(xsl, mode="rb") as xsl_file:
            xsl_string = xsl_file.read()

    xml = XML(xml_string)
    xsl = XML(xsl_string)
    transform = XSLT(xsl)
    print(xml_name)
    #print(tostring(transform(xml), pretty_print=True).decode("utf-8"))


    f =  open(xml_name, "wb")
    f.write(tostring(transform(xml), pretty_print=True, encoding="utf-8"))
    f.close()

if __name__ == '__main__':
    log_folder, output_folder, inspire_info = config_getParameters(config_file)
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    geodcatap_xml(inspire_info["xml_url"], inspire_info["xsl_url"])