# Databricks notebook source
spark.sql('create schema if not exists workspace.project_pipeline')
spark.sql("create volume if not exists project_pipeline.manual_file_project")

print("schema and volume are created successfully")