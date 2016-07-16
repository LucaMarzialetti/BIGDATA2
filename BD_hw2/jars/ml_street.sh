#!/bin/bash
#PBS -A train_bigdat16
#PBS -l walltime=24:00:00
#PBS -l select=1:ncpus=4:mem=8GB
#PBS -q parallel
#PBS -p 1020
module load autoload jre/1.8.0_73
current_folder=`pwd`/bd

#MACHINE-LEARNING_DATASET
class="crimes.MachineLearning"
jarfile="MachineLearning.jar"
input="dataset/MLDatasetSmall"
output="dataset/CrimeStreetML"

$HOME/spark-1.6.1-bin-hadoop2.6/bin/spark-submit --class $class --master local[4] $current_folder/$jarfile $current_folder/$input $current_folder/$output