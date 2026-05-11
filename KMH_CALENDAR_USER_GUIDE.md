# KMH Calendar User Guide

## What this page is for

The KMH Calendar is a standalone page for creating, viewing, rescheduling, and cancelling ZK-KMH flights in Envision.

It is built for local New Zealand time. Flights are shown in `Pacific/Auckland` time on the page, while Envision still stores the underlying data in UTC.

## Signing in

1. Open the KMH Calendar page.
2. Sign in with your Envision username and password.
3. After login, the board loads your flights from Envision.

If the page asks you to sign in again, the session has expired or the browser session was cleared.

## Board Views

The board supports three views:

- `Day` for a single day schedule
- `Week` for the operational week view
- `Month` for a wider overview

Use `Previous`, `Today`, `Next`, and `Jump To` to move around the schedule.

## Creating a Flight

1. Click `Add Flight`.
2. Enter the flight date, flight number, route, ETD, ETA, expected passenger count, expected cargo, flight type, pilot, and note.
3. Flight numbers must be in the format `3C1`, `3C2`, `3C3`, and so on.
4. Routes are limited to:
   - `CHT -> PIT`
   - `PIT -> CHT`
   - `CHT -> CHT`
   - `PIT -> PIT`
5. Click `Create In Envision`.

After a successful create:

- the flight number increments automatically
- the route swaps for the return leg
- the note clears so the next flight can be entered quickly

## Flight Cards

Each flight card shows:

- flight number
- crew code
- ETD and ETA
- route
- pilot name
- status

Click a flight card to open the full details window.

## Quick Actions

Each flight card also has quick actions:

- `Reschedule`
- `Cancel`

These quick actions only work for flights in `Planning`.

### Quick Reschedule

Use the quick reschedule popup to change:

- flight date
- ETD
- ETA

The change is sent directly to Envision.

### Quick Cancel

Cancelling a flight requires:

- a cancel code from Envision
- a reason for cancellation

The cancel code dropdown is loaded from Envision, and the reason is sent as the cancellation remarks.

## Flight Details Window

Click a flight card to open the detailed view. From there you can:

- review the full flight details
- view the note
- view the crew
- reschedule a planning flight
- cancel a planning flight
- change the pilot if the flight is still in planning

Non-planning flights cannot be edited or cancelled.

## Exporting Flights

Use `Export CSV` to export the flights currently shown on the board.

The export includes:

- flight date
- flight number
- route
- ETD
- ETA
- pilot
- expected passengers
- expected cargo
- note
- Envision flight ID

## Notes

- The page reads flight data from Envision, so any changes in Envision will appear on refresh.
- Expected passenger and cargo values are stored with the flight note metadata for reporting.
- The KMH page is separate from the main APG navigation.

## If Something Does Not Work

- Make sure you are signed in again if the page shows a login prompt.
- Check that the flight is still in `Planning` before trying to edit or cancel it.
- If canceling a flight fails, confirm you selected a valid cancel code and entered a reason.
- If a pilot does not appear, refresh the page so the pilot lookup reloads from Envision.

