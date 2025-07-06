# Databricks notebook source
datasets = [
    {"file_name" : "orders"},
    {"file_name" : "products"},
    {"file_name" : "customers"}
]

dbutils.jobs.taskValues.set("output_datasets",datasets)