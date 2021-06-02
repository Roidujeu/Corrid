#!/bin/bash
echo -n "Hello, Please enter the output folder name: "
read folName
rm -rf $folName
if mvn install;
then
	if hadoop jar "./target/Corrid-1.0.jar" "./dataset/dataset.csv" "./dataset/vaccinations.csv" $folName;
	then
		echo "All processes finished Successfully!"
	else
		echo "Failed to execute hadoop!"
	fi
else
	echo "Failed to install!"
fi
