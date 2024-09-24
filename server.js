const express = require('express');
const cors = require('cors');
const mqtt = require('mqtt');
const redis = require('redis');
const mongoose = require('mongoose');


const app = express();

app.use(cors()); 

app.use(express.json());

const redisClient = redis.createClient({
  host: 'localhost',
  port: 6379
});

redisClient.on('error', (err) => {
  console.error('Redis error:', err);
});

redisClient.on('close', () => {
  console.log('Redis connection closed');
});

const mqttClient = mqtt.connect('ws://broker.hivemq.com/mqtt', {
  protocol: 'ws',
});

// MongoDB 
mongoose.connect('mongodb://localhost:3000/todoDB', {
  useNewUrlParser: true,
  useUnifiedTopology: true,
});

const taskSchema = new mongoose.Schema({
  task: String,
});

const Task = mongoose.model('Task', taskSchema);

const TODO_KEY = 'FULLSTACK_TASK_RUPAM';

// MQTT 
mqttClient.on('connect', () => {
  console.log('Connected to MQTT Broker');
  mqttClient.subscribe('/add', (err) => {
    if (!err) {
      console.log('Subscribed to /add topic');
    }
  });
});

mqttClient.on('message', async (topic, message) => {
  if (topic === '/add') {
    let tasks = JSON.parse((await redisClient.get(TODO_KEY)) || '[]');
    tasks.push(message.toString());

    if (tasks.length > 50) {
      await Task.insertMany(tasks.map(task => ({ task })));
      tasks = []; // Clear tasks from cache after moving to MongoDB
    }

    await redisClient.set(TODO_KEY, JSON.stringify(tasks));
    console.log('Task added:', message.toString());
  }
});


app.get('/fetchAllTasks', async (req, res) => {
  try {
    let tasks = JSON.parse((await redisClient.get(TODO_KEY)) || '[]');

    if (tasks.length < 1) {
      tasks = await Task.find({});
    }

    res.json(tasks);
  } catch (err) {
    console.error('Error fetching tasks:', err);
    res.status(500).json({ error: 'Error fetching tasks' });
  }
});


const PORT = 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});