-- queries.sql (Revised to insert baomain_data)

-- name: GetUserByID :one
SELECT * FROM users
WHERE id = ? LIMIT 1;

-- name: GetUserByUsername :one
SELECT * FROM users
WHERE username_lowercase = ? LIMIT 1;

-- name: CreateUser :execresult
INSERT INTO users (
    username, password_hash, username_lowercase, baomain_data
) VALUES (
    ?, ?, ?, ?
);

-- name: CreatePlayer :exec
INSERT INTO players (
    user_id, name, color
) VALUES (
    ?, ?, ?
);

-- (The rest of the file is unchanged)
-- name: GetPlayerByUserId :one
SELECT * FROM players
WHERE user_id = ? LIMIT 1;

-- name: UpdateUserData :exec
UPDATE users
SET baomain_data = ?
WHERE id = ?;

-- name: UpdatePlayerBestScore :exec
UPDATE players
SET best_score = ?
WHERE id = ?;

-- name: GetTopScores :many
SELECT name, best_score
FROM players
ORDER BY best_score DESC
LIMIT ?
OFFSET ?;

-- name: GetPlayerByName :one
SELECT * FROM players
WHERE name LIKE ?
LIMIT 1;

-- name: GetPlayerRank :one
SELECT COUNT(*) + 1 as "rank" FROM players
WHERE best_score >= (
    SELECT best_score FROM players p2
    WHERE p2.id = ?
);

-- name: SetUserAdminStatus :exec
UPDATE users
SET is_admin = ?
WHERE username_lowercase = ?;

-- name: CreateUserBan :exec
INSERT INTO bans (banned_user_id, admin_user_id, reason, expires_at)
VALUES (?, ?, ?, ?);

-- name: CreateIPBan :exec
INSERT INTO bans (banned_ip_address, admin_user_id, reason, expires_at)
VALUES (?, ?, ?, ?);

-- name: GetActiveBanForUser :one
SELECT * FROM bans
WHERE banned_user_id = ? AND (expires_at IS NULL OR expires_at > NOW())
LIMIT 1;

-- name: GetActiveBanForIP :one
SELECT * FROM bans
WHERE banned_ip_address = ? AND (expires_at IS NULL OR expires_at > NOW())
LIMIT 1;

-- name: UpdateUserPassword :exec
UPDATE users
SET password_hash = ?
WHERE username_lowercase = ?;

-- Friendship Queries
-- name: GetFriendshipsForUser :many
SELECT sqlc.embed(users), f.status, f.action_user_id FROM friendships f
JOIN users ON (
    CASE
        WHEN f.user_one_id = ? THEN f.user_two_id
        ELSE f.user_one_id
    END
) = users.id
WHERE (f.user_one_id = ? OR f.user_two_id = ?) AND f.status IN ('pending', 'accepted');

-- name: CreateFriendRequest :exec
INSERT INTO friendships (user_one_id, user_two_id, status, action_user_id)
VALUES (?, ?, 'pending', ?);

-- name: UpdateFriendshipStatus :exec
UPDATE friendships
SET status = ?, action_user_id = ?
WHERE user_one_id = ? AND user_two_id = ?;

-- name: DeleteFriendship :exec
DELETE FROM friendships
WHERE user_one_id = ? AND user_two_id = ?;