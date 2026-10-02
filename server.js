const express = require('express');
const { createServer } = require('http');
const { WebSocketServer } = require('ws');
const path = require('path');

const app = express();
const server = createServer(app);
const wss = new WebSocketServer({ server });

// Serve static frontend files
app.use(express.static(path.join(__dirname, 'public')));

// Room management: roomCode -> Set of WebSocket clients
const rooms = new Map();

wss.on('connection', (ws) => {
  let currentRoom = null;

  ws.on('message', (message) => {
    try {
      const data = JSON.parse(message);

      if (data.type === 'join') {
        currentRoom = data.room;
        if (!rooms.has(currentRoom)) {
          rooms.set(currentRoom, new Set());
        }
        const room = rooms.get(currentRoom);
        
        if (room.size >= 2) {
          ws.send(JSON.stringify({ type: 'error', message: 'Room is full (max 2 users).' }));
          return;
        }

        room.add(ws);
        ws.send(JSON.stringify({ type: 'joined', room: currentRoom, peerCount: room.size }));
        
        // Notify other peer if present
        room.forEach(client => {
          if (client !== ws && client.readyState === ws.OPEN) {
            client.send(JSON.stringify({ type: 'peer_joined' }));
          }
        });
      } 
      else if (data.type === 'signal' || data.type === 'message') {
        // Relay encrypted payloads or signaling data to the other peer in the room
        if (currentRoom && rooms.has(currentRoom)) {
          const room = rooms.get(currentRoom);
          room.forEach(client => {
            if (client !== ws && client.readyState === ws.OPEN) {
              client.send(JSON.stringify(data));
            }
          });
        }
      }
    } catch (err) {
      console.error('Error processing message:', err);
    }
  });

  ws.on('close', () => {
    if (currentRoom && rooms.has(currentRoom)) {
      const room = rooms.get(currentRoom);
      room.delete(ws);
      room.forEach(client => {
        if (client.readyState === ws.OPEN) {
          client.send(JSON.stringify({ type: 'peer_left' }));
        }
      });
      if (room.size === 0) {
        rooms.delete(currentRoom);
      }
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Secure Relay Server running at http://localhost:${PORT}`);
});