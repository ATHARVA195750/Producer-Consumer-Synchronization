# PRODUCER AND CONSUMER

This repository demonstrates producer-consumer synchronization patterns.

## Overview

This project implements basic producer-consumer synchronization using:
- **Producer**: Generates data/items and adds them to a shared buffer
- **Consumer**: Retrieves data/items from the shared buffer and processes them

## Files

- `index.html` - HTML structure for the demo
- `style.css` - Styling for the interface
- `script.js` - JavaScript implementation of producer-consumer logic

## How It Works

1. The producer adds items to a shared queue/buffer
2. The consumer retrieves items from the queue when available
3. Synchronization ensures thread-safe access to the shared resource

## Running

Open `index.html` in a web browser to see the producer-consumer interaction in action.