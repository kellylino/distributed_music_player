# Distributed Audio Synchronization System

A distributed system for synchronizing audio playback across multiple nodes in real-time using peer-peer architecture and the Bully algorithm for leader election.

## 🎯 Overview

This system allows multiple computers to play synchronized audio with sub-second precision. One node acts as the leader, coordinating playback commands (play, pause, stop, resume), while follower nodes replicate these commands with timing compensation for network latency.

## ✨ Features

- **Distributed Synchronization**: Multiple nodes play audio in perfect sync
- **Leader Election**: Automatic leader election using Bully algorithm when leader fails
- **Fault Tolerance**: System continues working even if nodes disconnect
- **Automatic Discovery**: Nodes automatically discover each other on the network
- **State Synchronization**: New nodes automatically sync playback state with leader
- **Command Line Interface**: Easy-to-use CLI for controlling playback
- **Persistent Connections**: Efficient connection reuse for better performance

## 🏗️ Architecture

### System Components

1. **PeerNode** (`node.py`): Main node class coordinating all modules
2. **NetworkManager** (`network.py`): Handles peer-to-peer communication
3. **BullyElection** (`election.py`): Implements leader election algorithm
4. **MessageHandler** (`message_handler.py`): Processes incoming messages
5. **AudioPlayer** (`playback.py`): Manages audio playback
6. **Common Utilities** (`common.py`): Network communication helpers

### Message Types

The system uses JSON messages for communication:

- **Network Messages**: HELLO, DISCOVERY, HEARTBEAT, RECONNECT
- **Election Messages**: ELECTION, ELECTION_ANSWER, COORDINATOR
- **Playback Messages**: PLAY_REQUEST, PAUSE_REQUEST, RESUME_REQUEST, STOP_REQUEST
- **Sync Messages**: STATE_SYNC_REQUEST, STATE_SYNC_RESPONSE


## 📦 Installation & Requirements

### Prerequ