const express = require('express');
const app = express();
app.use(express.json());

// Fake tickets for now - later we connect real database
let tickets = [
  { id: 1, title: "Login not working", status: "Open" },
  { id: 2, title: "Page is slow", status: "In Progress" }
];

const VALID_STATUS = ["Open", "In Progress", "Resolved", "Closed"];

// GET all tickets - so you can see them
app.get('/tickets', (req, res) => {
  res.json(tickets);
});

// PATCH - STATUS CHANGE API - your task
app.patch('/tickets/:id/status', (req, res) => {
  const ticketId = Number(req.params.id);
  const { status } = req.body;

  // Rule 1: Get role from header/token, NOT from body
  // This is the fix we learned from Team No 1's PR review
  const userRole = req.headers['x-user-role'] || 'user';

  // Rule 2: Check if status is valid
  if (!VALID_STATUS.includes(status)) {
    return res.status(400).json({ error: "Invalid status. Use: Open, In Progress, Resolved, Closed" });
  }

  const ticket = tickets.find(t => t.id === ticketId);
  if (!ticket) {
    return res.status(404).json({ error: "Ticket not found" });
  }

  ticket.status = status;
  res.json({ message: "Status updated", ticket, updatedByRole: userRole });
});

app.listen(3000, () => console.log("Server running on port 3000"));
