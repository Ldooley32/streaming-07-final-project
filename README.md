# streaming-07-final-project
### Dayton Energy Consumption 

## Introduction: 
This project will use data on Dayton, Ohio's energy concumption that was found on the internet to similate a streaming data system. The message broker Rabbit MQ will be used and a producer will be designed to send the data to a queue. A consumer will be designed to retrieve the data and modify using python coding that will be log the modified data into a csv file.

# *** Source: 
    https://www.kaggle.com/datasets/robikscube/hourly-energy-consumption?resource=download
    

## External Dependencies
PIka
Pandas


## Version Control with Git

1. Send to gGitHub 

    ```    
        git add . 
        git commit -m "comment" 
        git push origin main
    ```

2. Retreive from GitHub 
    
    ```
        git checkout main git pull --rebase origin
    ```
 
3. Create and activate the project virtual environment. 

    ```
        python3 -m venv .venv 
        source .venv/bin/activate
    ```

3. Install all required packages into your local project virtual environment from requirements.txt file 

    ```
         python3 -m pip install -r requirements. txt
    ```

4. Add a .gitignore file to your project with useful entries. Use .gitignore example.


## Project 

# Producer

- Sends a MW entry in the data every 5 seconds with a timestamp to the queue named dayton_queue1 

- script will end when all data is retrieved. 

- To call script: 
```phython3 emitter_of_tasks.py```


# Consumer

- Collect the data in the queue and modify the message. Modifications are:
    1. separate the timestamp from the mega watts. 
    2. Dived the mega watts by the fictional amount of cost

- Continuously run until canceled. CTL + c to cancel
- to call script 
``` python3 listener_worker.py```



<img width="1483" alt="image" src="https://github.com/Ldooley32/streaming-07-final-project-/assets/140924268/2f2deabd-184a-4c7a-85a6-dd3c6e596a53">




<img width="1209" alt="image" src="https://github.com/Ldooley32/streaming-07-final-project-/assets/140924268/fd70d280-1f29-4bfa-bda7-7bed5567dd83">





<img width="1093" alt="image" src="https://github.com/Ldooley32/streaming-07-final-project-/assets/140924268/1f4dbf5d-6971-4ee8-8497-0d63c71d3c08">















