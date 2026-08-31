# System Design: Notification System

*A capstone system design walkthrough — designing a general-purpose notification system end to end — covering the domain model for notifications and user preferences, the fan-out and templating pipeline, multi-channel delivery (push, email, SMS, in-app), rate limiting and digesting to avoid overwhelming users, idempotency and exactly-once-delivery-effect guarantees, provider failover, and the specific delivery-guarantee and preference-respecting demands that make a notification system deceptively hard to get right at scale.*

---

## Table of Contents

1. [Introduction](#introduction)
2. [Why a Notification System Is a Different Kind of Hard](#1-why-a-notification-system-is-a-different-kind-of-hard)
3. [The Core Domain Model](#2-the-core-domain-model)
4. [The Notification Event Log: The Source of Truth for What Was Sent](#3-the-notification-event-log-the-source-of-truth-for-what-was-sent)
5. [Idempotency: The Single Most Important Property](#4-idempotency-the-single-most-important-property)
6. [Triggering and Fan-Out](#5-triggering-and-fan-out)
7. [Templating and Localization](#6-templating-and-localization)
8. [User Preferences and Consent](#7-user-preferences-and-consent)
9. [The Notification State Machine](#8-the-notification-state-machine)
10. [Multi-Channel Delivery and Provider Failover](#9-multi-channel-delivery-and-provider-failover)
11. [Rate Limiting, Batching, and Digests](#10-rate-limiting-batching-and-digests)
12. [Handling Bounces, Unsubscribes, and Dead Endpoints](#11-handling-bounces-unsubscribes-and-dead-endpoints)
13. [Data Security and Compliance](#12-data-security-and-compliance)
14. [Consistency, Availability, and the CAP Trade-off for Notifications](#13-consistency-availability-and-the-cap-trade-off-for-notifications)
15. [Scaling the System](#14-scaling-the-system)
16. [Observability for a Notification System](#15-observability-for-a-notification-system)
17. [Common Pitfalls](#16-common-pitfalls)
18. [Quick Reference Table](#quick-reference-table)
19. [Conclusion](#conclusion)

---

## Introduction

A notification system takes the general system design vocabulary covered in this series' System Design guide — event-driven pipelines, templating, queues, external API integration — and applies it to a problem that looks simple from the outside (send a message when something happens) but accumulates genuine complexity fast: multiple channels with wildly different delivery semantics and failure modes, per-user preferences that must be respected exactly, deduplication across a system that will inevitably retry, and the reputational cost of getting any of this wrong at scale (a duplicate email, a notification sent after a user unsubscribed, a critical alert silently dropped). This guide walks through designing such a system end to end, drawing directly on this series' Event-Driven Architecture, Rate Limiting, Secret Management, and Data Privacy guides, each of which turns out to be load-bearing infrastructure for a notification system that's actually trustworthy at scale, rather than optional architectural polish.

```plaintext
Triggering Event → Notification Service → [preferences check, templating] → Channel Router
                                                                                    ↓
                                                          Push | Email | SMS | In-App (provider APIs)
                                                                                    ↓
                                                          Delivery Log (source of truth) → Status callbacks
```

---

## 1. Why a Notification System Is a Different Kind of Hard

### It sits downstream of every other system, and inherits all of their event volume and bugs

Most systems covered in this series own their own event volume and can shape it deliberately. A notification system, by design, is triggered by every other system in a company's architecture — an order service, a security system, a billing pipeline, a social feature — each with its own bugs, retry behavior, and burst patterns. A bug anywhere upstream (a retry loop in the order service, say) becomes a notification-volume problem here, which is precisely why idempotency (Section 4) and rate limiting (Section 10) are this system's own responsibility, not something it can assume upstream systems will get right on its behalf.

### Different channels have fundamentally different delivery guarantees and costs

```plaintext
Push notification: cheap, fast, but not guaranteed — a stale device token silently fails.
Email: reliable delivery infrastructure exists (SMTP, provider APIs), but delivery
  still isn't instant or guaranteed (spam filters, bounces).
SMS: highly reliable but genuinely costly per message, and carrier-dependent latency varies widely.
```

Unlike a system that speaks to one downstream API, a notification system routes across channels with meaningfully different cost, latency, and reliability profiles — this is why channel selection and fallback (Section 9) is treated as a first-class routing decision here, not a simple "try channel X" step.

### The cost of getting a user's preferences wrong is a trust problem, not just a bug

A critical, freeing realization for the design that follows: a notification system, in the overwhelming majority of real-world designs, is not the source of truth for *why* a notification should be sent — that judgment belongs to the triggering system. Its job is to respect exactly what the user has consented to receive, deliver reliably across whichever channel that consent covers, and never notify someone who opted out — getting this wrong even occasionally (a marketing email after unsubscribe, a notification outside a user's configured quiet hours) causes damage disproportionate to how "small" the individual bug might look, both to user trust and, in regulated contexts (Section 12), to actual legal exposure.

---

## 2. The Core Domain Model

### Modeled with DDD, per this series' companion guide

```csharp
public record NotificationId(Guid Value);
public record UserId(Guid Value);

public enum NotificationChannel { Push, Email, Sms, InApp }
public enum NotificationStatus { Pending, Queued, Sent, Delivered, Failed, Suppressed }

public class Notification // the AGGREGATE ROOT, per this series' DDD guide
{
    public NotificationId Id { get; }
    public UserId Recipient { get; }
    public string TemplateKey { get; }
    public NotificationChannel Channel { get; }
    public NotificationStatus Status { get; private set; }
    private readonly List<NotificationEvent> _domainEvents = new();

    public void MarkSent(string providerMessageId)
    {
        if (Status != NotificationStatus.Queued)
            throw new InvalidOperationException($"Cannot mark sent from status {Status}");
        Status = NotificationStatus.Sent;
        _domainEvents.Add(new NotificationSentEvent(Id, providerMessageId));
    }

    public void Suppress(string reason)
    {
        if (Status != NotificationStatus.Pending)
            throw new InvalidOperationException($"Cannot suppress from status {Status}");
        Status = NotificationStatus.Suppressed;
        _domainEvents.Add(new NotificationSuppressedEvent(Id, reason));
    }
}
```

This directly applies this series' DDD guide's aggregate pattern — `Notification` is the aggregate root, enforcing its own state transitions (a notification can't be marked delivered before it's sent) rather than trusting every caller to check status before mutating it, and raising domain events at exactly the points those transitions genuinely occur.

### Separating the trigger, the notification, and the delivery attempt

```csharp
public record NotificationRequest(Guid IdempotencyKey, UserId Recipient, string EventType, IReadOnlyDictionary<string, string> TemplateData);
public record DeliveryAttempt(NotificationId NotificationId, NotificationChannel Channel, int AttemptNumber, DeliveryOutcome Outcome, DateTimeOffset AttemptedAt);
```

As covered in this series' DDD guide's aggregate-sizing discussion, keeping the inbound `NotificationRequest` (what a triggering system asked for), the `Notification` (the durable record of what this system decided to do about it, per-channel), and `DeliveryAttempt` (an immutable record of each individual send attempt) as separate entities means a single logical notification can fan out to multiple channels, each independently retried and tracked, without conflating "did we decide to notify this user" with "did any specific attempt actually succeed."

---

## 3. The Notification Event Log: The Source of Truth for What Was Sent

### Why "notification sent" can't just be a boolean flag

```sql
-- ❌ A single mutable "sent" boolean has no record of WHICH channel, WHEN, with what content,
--    or whether it was retried — useless for a support ticket or a compliance audit
UPDATE notifications SET sent = true WHERE id = 1;
```

A notification system needs an immutable, detailed record of every notification decision and every delivery attempt — not just whether something was "sent," but what template and data were used, which channel, when, and what the provider's response was. A mutable flag, overwritten in place, destroys exactly the detail a support investigation ("why didn't I get notified?") or a compliance audit depends on.

### The log as the append-only backbone

```sql
CREATE TABLE notification_log (
    sequence_id BIGINT PRIMARY KEY,
    notification_id UUID NOT NULL,
    idempotency_key UUID NOT NULL,     -- ties back to the ORIGINAL trigger, per Section 4
    recipient_id UUID NOT NULL,
    channel VARCHAR NOT NULL,
    event_type VARCHAR NOT NULL,       -- Requested, Queued, Sent, Delivered, Failed, Suppressed
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

In practice this table's role is usually filled by a distributed log (Kafka/Pulsar) rather than a relational table directly — every stage of a notification's life, from initial request through final delivery outcome, is first durably appended to the log. This gives a durable, replayable record (answer "what did we send this user, and when" definitively, for support or audit), and a backbone for downstream consumers via the **outbox/CDC pattern**, directly echoing this series' Event-Driven Architecture guide's discussion of avoiding dual-write inconsistency between "update notification state" and "publish the event."

### Derived views (inbox, notification center) are always recomputable from the log

```plaintext
An in-app "notification center" list is a DERIVED, materialized projection of this
  log, per this series' CQRS discussion — never a separately-maintained table that
  could drift out of sync with what was actually sent and delivered.
```

Per this series' Caching and Materialized View guides, a user-facing notification history view is a reasonable, even necessary, optimization for read performance, but it must always be treated as a derived projection of the log's truth — rebuildable from the log if it's ever suspected to have drifted, never the authoritative record itself.

---

## 4. Idempotency: The Single Most Important Property

### Why this is even more critical here, given Section 1's "downstream of everyone" reality

As covered throughout this series' RabbitMQ, Kafka, and Event-Driven Architecture guides, every messaging technology provides at-least-once delivery, and every upstream caller can time out ambiguously and retry — for a notification system specifically, an un-idempotent trigger handler means a retried "order shipped" event genuinely sends the same user the same email twice, which is a small but real trust cost multiplied across every retry, of every event, from every upstream system this service serves.

### Idempotency keys, supplied by the triggering system

```csharp
public async Task<NotificationResult> RequestNotificationAsync(NotificationRequest request)
{
    var existing = await _idempotencyStore.GetResultAsync(request.IdempotencyKey);
    if (existing is not null)
    {
        return existing; // the SAME result as the original request, no new notification created
    }

    var notification = await _notificationService.ProcessAsync(request);
    await _idempotencyStore.SaveResultAsync(request.IdempotencyKey, notification);
    return notification;
}
```

This is the concrete implementation of the idempotency pattern introduced generally in this series' Redis guide's rate-limiting section and REST guide's discussion — the triggering system supplies a unique idempotency key per *logical* event (e.g., derived from `order_id + "shipped"`, not regenerated on retry), and this service checks whether that key has already produced a notification before creating a new one. Since this system has many upstream callers, it should also document and enforce this contract clearly at the API boundary — an idempotency key is not optional metadata here, it's a required field.

### Idempotency through the fan-out and delivery layers, not just at the API boundary

```plaintext
Trigger → API (idempotency key checked here)
             → Fan-out per channel (a database constraint prevents a duplicate
                (idempotency_key, channel) row from ever being created twice)
             → Provider send (many providers accept their OWN idempotency key,
                per this series' Event-Driven Architecture guide's "idempotent
                at every hop" principle)
```

Idempotency needs to be enforced at every hop — the fan-out layer should have a database constraint preventing a duplicate `(idempotency_key, channel)` combination from ever producing two `Notification` records, and where the downstream provider (an email or SMS API) supports its own idempotency key, that should be used too, since a retried call to the provider itself is exactly the kind of ambiguous-timeout scenario this whole guide assumes as a baseline.

---

## 5. Triggering and Fan-Out

### The trigger contract: a stable event schema upstream systems can rely on

```csharp
public record TriggerEvent(string EventType, Guid IdempotencyKey, UserId Recipient, IReadOnlyDictionary<string, string> Data);
// e.g. EventType = "order.shipped", Data = { "order_id": "...", "tracking_url": "..." }
```

As covered in this series' Event-Driven Architecture and API Design guides, a notification system's most important interface decision is the shape of the trigger contract itself — a stable, versioned event schema that upstream systems publish to (directly or via a shared event bus), decoupling "something happened" from "how it gets communicated," which is what lets the notification templates, channels, and preferences evolve independently of the systems that trigger them.

### Fan-out: one logical event, potentially several channel-specific notifications

```plaintext
"order.shipped" → fan out to: push notification (if enabled), email (if enabled),
  in-app notification (always, low cost) — each becomes its OWN Notification
  record (Section 2), independently tracked, retried, and delivered.
```

Per this series' Fan-Out pattern discussion, a single triggering event commonly needs to become multiple channel-specific notifications, each subject to its own preference check (Section 7), its own template (Section 6), and its own delivery/retry lifecycle (Section 9) — modeling this as an explicit fan-out step, rather than conflating "one event" with "one notification," is what makes independent per-channel tracking and failure handling possible.

### Priority classification at fan-out time

```plaintext
Not every triggered notification is equally urgent — a security alert ("new login
  from an unrecognized device") and a marketing digest both flow through the same
  pipeline but need very different rate-limiting, digesting (Section 10), and
  delivery-guarantee treatment.
```

As covered in this series' Priority Queue pattern discussion, classifying notifications by priority at fan-out time (transactional/security-critical vs. informational vs. marketing) lets every downstream stage — rate limiting, digesting, channel fallback — apply the right policy per notification rather than treating a password-reset email and a weekly-digest email identically.

---

## 6. Templating and Localization

### Separating content from code so non-engineers can safely change copy

```plaintext
Templates (per this series' Templating Engine discussion) are stored and versioned
  separately from the notification pipeline's code — a marketing or support team
  editing notification copy shouldn't require a code deployment, but SHOULD go
  through the same review/approval workflow as any other user-facing change.
```

As covered in this series' Content Management and Templating guides, decoupling template content from the pipeline's deployment cycle lets copy be iterated on independently, while still keeping template changes behind a review process — an unreviewed template change here is a genuine risk vector (broken merge fields, unintended tone in a sensitive notification type).

### Rendering with the fan-out event's data, and validating before send

```csharp
var rendered = _templateEngine.Render(templateKey, request.Data, recipient.LocalePreference);
if (!_templateValidator.IsValid(rendered))
{
    await SuppressAndAlertAsync(notification, "template rendering produced invalid output");
}
```

Per this series' Data Validation guide's discipline, rendered output should be validated before being handed to a delivery provider — a missing merge field (an empty `{{tracking_url}}`) reaching a real user is a small but avoidable failure that validation at render time catches before send, rather than after a user reports a broken notification.

### Localization as a first-class template dimension, not an afterthought

```plaintext
Templates are keyed by (template_key, locale) — a recipient's locale preference
  (Section 7) determines which rendered variant is used, per this series'
  Internationalization guide's discussion of avoiding a single-locale default
  that quietly degrades the experience for a meaningful fraction of users.
```

Treating locale as a first-class template dimension from the start — rather than retrofitting translation onto a system designed around one language — avoids the common pattern this series' Internationalization guide warns against, where localization becomes a large, disruptive refactor rather than a natural extension of an already-locale-aware template system.

---

## 7. User Preferences and Consent

### Preferences as the gate every notification must pass through before delivery

```csharp
public async Task<bool> IsAllowedAsync(UserId user, string notificationCategory, NotificationChannel channel)
{
    var prefs = await _preferenceStore.GetAsync(user);
    if (prefs.OptedOutOf(notificationCategory, channel)) return false;
    if (prefs.IsInQuietHoursAsync(channel) && notificationCategory != "security_critical") return false;
    return true;
}
```

As covered in this series' User Preferences and Data Privacy guides, every notification — regardless of how urgent the triggering system considers it — passes through an explicit preference check before any delivery attempt, with a narrow, deliberately-scoped exception for genuinely critical categories (security alerts, account-safety notifications) that a system may be permitted to override quiet hours or channel opt-outs for, per its own clearly-documented policy, never silently.

### Preferences as their own aggregate, changeable independently of any notification

```csharp
public class NotificationPreferences // its OWN aggregate, per this series' DDD guide
{
    public UserId Owner { get; }
    private readonly Dictionary<(string Category, NotificationChannel Channel), bool> _optIns = new();
    public QuietHours QuietHours { get; private set; }

    public void OptOut(string category, NotificationChannel channel) => _optIns[(category, channel)] = false;
}
```

Modeling preferences as their own aggregate — read by, but not owned by, the notification pipeline — means a user can manage their preferences at any time, independent of any specific notification in flight, and the preference check (above) always reads the current, authoritative state rather than a snapshot that could have gone stale between when a trigger fired and when delivery was attempted.

### Consent state changes must take effect immediately, not eventually

```plaintext
Unlike most read paths in this system (Section 13), the preference check MUST read
  strongly consistent, current state — a user who just unsubscribed should never
  receive one more notification because the preference check read a stale replica.
```

This is a deliberate, explicit exception to this guide's general eventual-consistency posture (Section 13): given Section 1's framing of preference violations as a trust and compliance problem, not just a bug, the preference check is one of the few reads in this system that must be strongly consistent, even at some latency cost.

---

## 8. The Notification State Machine

### An explicit, enumerable set of states and legal transitions

```plaintext
Pending → (Suppressed, if preferences deny it) | Queued → Sent → Delivered
                                                      ↓
                                                    Failed
```

As covered in Section 2's `Notification` aggregate, a notification's lifecycle is a small, explicit state machine — and the aggregate's own methods (`MarkSent()`, `Suppress()`) are what enforce that only legal transitions are ever possible, throwing rather than silently succeeding if called out of order (marking a notification delivered before it was ever sent, for instance).

### Why "Suppressed" is a first-class terminal state, not a silent no-op

```csharp
public void Suppress(string reason)
{
    if (Status != NotificationStatus.Pending)
        throw new InvalidOperationException($"Cannot suppress from status {Status}");
    Status = NotificationStatus.Suppressed;
    _domainEvents.Add(new NotificationSuppressedEvent(Id, reason)); // LOGGED, not silently dropped
}
```

Given Section 1's emphasis on preference violations being a trust problem, a notification correctly withheld because a user opted out deserves the same auditable record as one that was sent — "we correctly did not notify this user, and here's why" is exactly the kind of answer a support investigation or compliance audit needs to be able to produce, which is why suppression is modeled as an explicit, logged state transition rather than the request simply disappearing.

### Delivered vs. Sent — a distinction most channels can't actually confirm equally well

```plaintext
Sent: the provider accepted the message for delivery.
Delivered: the provider CONFIRMED it reached the recipient's device/inbox —
  reliably available for push (via device ack) and increasingly for email
  (via provider webhooks), but often NOT reliably available for SMS depending on carrier.
```

Worth being explicit about a real limitation, per this series' Notification Systems guide: "Delivered" should only be used where the channel and provider genuinely support delivery confirmation — for channels where that signal isn't reliably available, the honest state to track is "Sent, delivery unconfirmed," rather than inferring "Delivered" from the absence of a bounce, which would overstate the system's actual certainty.

---

## 9. Multi-Channel Delivery and Provider Failover

### Each channel behind its own adapter, normalized to a common interface

```csharp
public interface IChannelProvider
{
    Task<DeliveryOutcome> SendAsync(Notification notification, RenderedContent content);
}
// PushProvider, EmailProvider, SmsProvider each implement this against their
// respective vendor APIs (FCM/APNs, SendGrid/SES, Twilio, etc.)
```

Per this series' Adapter Pattern and API Integration guides, normalizing each channel's very different provider API behind one common interface is what lets the fan-out and retry logic (Section 4, Section 11) stay channel-agnostic — the adapter absorbs each provider's specific request/response shape, error codes, and rate-limit behavior.

### Provider failover within a channel, not just across channels

```plaintext
Primary email provider degraded/rate-limited → failover to a SECONDARY email
  provider, per this series' Resilience guide's fallback-chain pattern — distinct
  from Section 8's channel-level fallback (email → push), this is provider-level
  redundancy WITHIN the same channel.
```

As covered in this series' Resilience guide, relying on a single provider per channel is a real availability risk at scale — providers have their own outages and rate limits — so a mature notification system maintains at least one fallback provider per high-volume channel, with the adapter layer (above) making the switch transparent to the rest of the pipeline.

### Delivery attempts as their own immutable record, supporting exactly this kind of retry/failover history

```csharp
public record DeliveryAttempt(NotificationId NotificationId, string ProviderId, int AttemptNumber, DeliveryOutcome Outcome, DateTimeOffset AttemptedAt);
```

Per Section 2's separation of the `Notification` aggregate from individual `DeliveryAttempt` records, this is exactly where that separation pays off — a notification that failed on the primary provider, succeeded on a fallback, has a complete, auditable history of both attempts, rather than a single mutable "last attempt" field that would lose the story of what actually happened.

---

## 10. Rate Limiting, Batching, and Digests

### Per-user rate limiting to prevent notification floods

```csharp
if (!await _rateLimiter.TryAcquireAsync($"user:{userId}:channel:{channel}"))
{
    await QueueForDigestAsync(notification); // per below, rather than dropping or force-sending
}
```

Per this series' Redis-backed rate limiting guide's token-bucket/sliding-window patterns, applied per user and per channel, a burst of triggering events (a busy day with many order updates, say) shouldn't translate into a burst of individually-delivered notifications hitting a user in the same window — this is the notification-system analog of the alert-fatigue problem covered in this series' Investment Monitoring system design guide, applied here across arbitrary notification categories rather than just financial alerts.

### Digesting: combining several notifications into one, deliberately

```plaintext
Rather than dropping or delaying excess notifications silently, rate-limited
  notifications are QUEUED and periodically combined into a single digest
  notification ("You have 4 new updates") — the user still gets informed,
  just at a controlled cadence rather than as individual interruptions.
```

As covered in this series' Batching pattern discussion, digesting is the deliberate alternative to either dropping excess notifications (losing information) or delivering every one individually (overwhelming the user) — it trades immediacy for volume control, and per Section 5's priority classification, should generally exempt genuinely time-sensitive/critical notifications from digesting rather than applying the same cadence to every category uniformly.

### User-configurable digest cadence, echoing Section 7's preference model

```plaintext
Per this series' User Preferences guide: digest frequency (immediate, hourly,
  daily) should itself be a preference a user controls per category — the "right"
  cadence is genuinely user-dependent, the same principle this series' Investment
  Monitoring guide applies to alert cooldowns.
```

Digest cadence is naturally modeled as an extension of Section 7's preference aggregate rather than a separate system, since it's the same underlying question — "how much, and how often, does this user want to hear from this category" — just answered with a frequency setting instead of a binary opt-in/opt-out.

---

## 11. Handling Bounces, Unsubscribes, and Dead Endpoints

### Treating a bounce or an unsubscribe as a signal that must update state immediately, not just a delivery-attempt outcome

```csharp
public async Task HandleProviderWebhookAsync(ProviderWebhookEvent evt)
{
    if (evt.Type == WebhookEventType.HardBounce)
        await _endpointStore.MarkDeadAsync(evt.RecipientEndpoint); // per Section 7's consistency requirement
    if (evt.Type == WebhookEventType.Unsubscribe)
        await _preferenceStore.OptOutAsync(evt.UserId, evt.Category, evt.Channel); // same urgency as Section 7
}
```

A hard bounce (an email address that no longer exists) or a provider-reported unsubscribe isn't just a failed delivery attempt to log — it's new information about the recipient's endpoint or consent that must update state immediately, with the same strong-consistency requirement Section 7 places on preference reads, since continuing to send to a dead or opted-out endpoint after receiving this signal repeats exactly the trust failure Section 1 warns against.

### Verifying webhook authenticity from providers, per this series' security discipline

```csharp
var isValid = _webhookSignatureVerifier.Verify(payload, signature, _providerWebhookSecret);
if (!isValid) return Unauthorized(); // per this series' Secret Management and OWASP Top 10 guides
```

As covered in this series' Secret Management and OWASP Top 10 guides, a provider webhook endpoint is a publicly reachable URL, by necessity — without verifying the provider's cryptographic signature on every incoming webhook, an attacker could submit a forged unsubscribe or bounce event to suppress notifications a real user should have received, which is a subtle but genuine denial-of-service vector specific to this kind of externally-triggered state change.

### Suppression lists: a durable record of endpoints that should never be sent to again

```plaintext
Hard-bounced addresses and complained-about senders go on a durable, checked-before-every-send
  suppression list, per this series' Email Deliverability discussion — this ALSO protects
  the system's own sender reputation with providers, not just the individual recipient.
```

Beyond respecting the individual user, maintaining a suppression list checked before every send protects the notification system's own standing with email/SMS providers — a provider that sees a sender repeatedly emailing hard-bounced or complained-about addresses will degrade that sender's overall deliverability, affecting every other user's notifications too.

---

## 12. Data Security and Compliance

### PII in notification content and the systems that render it

Notification content routinely includes personally identifiable information (names, order details, account activity) — the practical strategy mirrors this series' Secret Management and Data Privacy guides' least-privilege and data-minimization principles: templates and logs are designed to avoid embedding more PII than the notification's purpose requires, and the notification log (Section 3) itself is subject to a defined retention policy rather than kept indefinitely.

### Regulatory consent requirements (CAN-SPAM, TCPA, GDPR, and similar)

```plaintext
Marketing notifications, in particular, are subject to real regulatory requirements
  around consent, unsubscribe mechanisms, and record-keeping (CAN-SPAM for email,
  TCPA for SMS/calls in the US, GDPR consent requirements more broadly) — this is
  a genuine legal compliance question the preference model (Section 7) must be
  built to satisfy, not just a UX nicety.
```

Worth flagging explicitly, distinct from the purely technical design: several jurisdictions impose specific, binding requirements on marketing-category notifications — a functioning unsubscribe mechanism, proof of consent, and record-keeping of consent state changes — and Section 7's preference aggregate and Section 11's suppression handling need to be built with these requirements as real constraints, ideally reviewed with legal/compliance, not assumed satisfied by "we have a preferences table."

### Encryption and secret management for provider credentials

```csharp
// Provider API keys (push, email, SMS vendors) are exactly the kind of secret covered
// in this series' Secret Management guide — never in source control, rotated regularly
var providerApiKey = await _secretClient.GetSecretAsync("email-provider-api-key");
```

Every principle covered in this series' Secret Management guide applies directly here: no hardcoded credentials, Managed Identity where the platform supports it, and rotation discipline for anything that could let an attacker send notifications as this system or intercept delivery webhooks.

---

## 13. Consistency, Availability, and the CAP Trade-off for Notifications

### Why most of the pipeline favors availability, with two deliberate, narrow exceptions

As covered in this series' System Design guide's CAP theorem discussion, most of a notification system's pipeline can, and should, favor availability and eventual consistency — a notification arriving a few seconds later than technically possible, or a notification-center view being briefly stale, costs very little. Sections 7 and 11 are the deliberate exceptions: preference and suppression-list reads need strong consistency specifically because the cost of getting them wrong (notifying someone who opted out) is asymmetric and trust-damaging in a way ordinary delivery latency isn't.

### Where eventual consistency is the correct, deliberate default

```plaintext
Fan-out and delivery queueing (Section 5, Section 9) → eventual consistency is fine,
  a few seconds of delay is invisible to the user
Notification-center / history views (Section 3) → eventual consistency, standard CQRS
Preference reads before send (Section 7) → STRONG consistency, the deliberate exception
Suppression-list checks before send (Section 11) → STRONG consistency, same reasoning
```

This split — eventual consistency as the default, with two narrow, explicitly-justified exceptions — is a more nuanced position than either "everything eventually consistent" or "everything strongly consistent," and reflects this guide's core argument (Section 1) that the actual risk in a notification system isn't primarily about raw delivery speed, it's about respecting what a user has explicitly told the system not to do.

---

## 14. Scaling the System

### Applying this series' System Design guide's building blocks, with notification-specific emphasis

```plaintext
Queue-based decoupling (per this series' RabbitMQ/Kafka guides): fan-out, templating,
  and delivery each run as separately-scaled, queue-decoupled stages, so a slow
  provider doesn't block fan-out for other channels or other users
Provider-specific rate limit awareness (per this series' Rate Limiting guide):
  each channel adapter respects ITS provider's own rate limits, queueing rather
  than hammering a provider into throttling the whole channel
Read replicas (per this series' PostgreSQL guide): safe for notification-history
  and analytics queries — never for the preference/suppression reads Section 13
  requires to stay strongly consistent
```

Every technique from this series' System Design guide applies here, with the caveat that each one needs to be evaluated against Section 13's two consistency exceptions before being applied — a caching layer that would be a reasonable read-scaling technique elsewhere is specifically the wrong tool for the preference and suppression checks.

### Horizontal scaling of channel-specific worker pools

```plaintext
Push, email, and SMS delivery workers scale INDEPENDENTLY (per this series'
  Background Services guide) — SMS volume and email volume rarely move together,
  and provisioning them identically wastes capacity on whichever channel is
  currently quieter.
```

Per this series' Background Services guide, scaling each channel's delivery worker pool independently — rather than one generic "notification worker" pool — matches provisioning to each channel's actual, often uncorrelated, volume pattern.

---

## 15. Observability for a Notification System

### Every guide in this series' observability trio, applied with notification-specific stakes

```plaintext
Structured logs (per this series' Structured Logging guide): every state transition
  (Section 8), every suppression and its reason, every delivery attempt and outcome —
  with idempotency key and notification ID for correlation, NEVER logging full
  notification content containing PII at excessive verbosity
Distributed tracing (per this series' Distributed Tracing guide): tracing a single
  triggering event's journey through fan-out, templating, and delivery — essential
  for diagnosing why a specific user didn't receive an expected notification
Metrics (per this series' Prometheus/Grafana guide): delivery success rate per
  channel and per provider, suppression rate (and reasons), digest queue depth,
  provider failover frequency — the aggregate health signals an on-call engineer
  watches continuously
```

Every technique from this series' observability guides applies directly, with one notification-specific addition worth stating explicitly: suppression rate, broken down by reason (preference opt-out, quiet hours, suppression list), is itself a meaningful health signal — a sudden spike can indicate a genuine upstream problem (a misconfigured category default) well before it would show up as a user complaint.

### Alerting on delivery-health symptoms

```promql
# Per this series' Prometheus/Grafana guide's symptom-based alerting principle
rate(notification_delivery_failed_total{channel="email"}[5m]) / rate(notification_delivery_attempted_total{channel="email"}[5m]) > 0.10
```

A sudden spike in delivery failure rate on a specific channel or provider, or an unexpected spike in suppression rate, is exactly the kind of symptom this series' Prometheus/Grafana guide argues alerts should be built around — per-channel and per-provider granularity matters here specifically because a single degraded provider can hide behind an otherwise-healthy aggregate delivery rate until it's already meaningfully affecting users on that channel.

---

## 16. Common Pitfalls

| Pitfall | Why it hurts | Better approach |
|---|---|---|
| No idempotency key required from triggering systems | A retried upstream event genuinely sends the same notification twice | Require and enforce idempotency keys at the API boundary, checked before fan-out |
| Treating "sent" as equivalent to "delivered" | Overstates the system's actual delivery certainty, especially for SMS | Track "Sent" and "Delivered" as distinct states; only claim delivery where the channel genuinely confirms it |
| Caching or eventually-consistent reads of user preferences before send | A user who just unsubscribed can still receive one more notification | Preference and suppression-list checks read strongly consistent, current state |
| Silently dropping suppressed notifications | No auditable answer to "why didn't I get notified" for support or compliance | Model suppression as an explicit, logged terminal state with a reason |
| A single provider per channel with no failover | A provider outage or rate-limit event becomes a full channel outage | Maintain at least one fallback provider per high-volume channel behind a common adapter interface |
| No rate limiting or digesting per user | A burst of triggering events becomes a burst of individually-delivered notifications, overwhelming the user | Per-user, per-channel rate limiting with digesting as the deliberate alternative to dropping or flooding |
| Not verifying provider webhook signatures | An attacker can forge bounce/unsubscribe events to suppress real notifications | Always verify cryptographic signatures on incoming provider webhooks |
| Assuming a "preferences table" satisfies regulatory consent requirements | Marketing notifications carry real legal exposure (CAN-SPAM, TCPA, GDPR) if consent/record-keeping isn't genuinely compliant | Build the preference and suppression model against actual regulatory requirements, reviewed with legal/compliance |

---

## Quick Reference Table

| Concept | Purpose |
|---|---|
| `Notification` aggregate + state machine | Enforces only legal notification state transitions, including auditable suppression, per this series' DDD guide |
| Append-only notification log | The provable, detailed record of what was sent, when, and why (or why not) |
| Idempotency key from the triggering system | Prevents duplicate notifications from routine upstream retries |
| Fan-out with priority classification | Turns one triggering event into independently-tracked, appropriately-prioritized per-channel notifications |
| Strongly consistent preference and suppression checks | The deliberate exception to eventual consistency, protecting user trust and legal compliance |
| Channel adapters with provider failover | Normalizes very different provider APIs and protects against single-provider outages |
| Rate limiting + digesting | Prevents notification floods without silently dropping information |
| Verified provider webhooks for bounces/unsubscribes | Keeps suppression state accurate and protects against forged suppression attacks |

---

## Conclusion

A notification system takes every general system design technique covered throughout this series and applies it to a problem whose real difficulty is easy to underestimate — because "send a message when something happens" hides a genuine amount of complexity once idempotency, multi-channel delivery, rate limiting, and above all, exact respect for user consent, all have to hold simultaneously and reliably at scale. The design that actually holds up under that reality rests on a small number of non-negotiable foundations: an append-only log that can answer "what did we send, and why" definitively; idempotency enforced at the API boundary and through every fan-out and delivery hop; a preference and suppression model that is deliberately, narrowly exempted from this system's otherwise eventually-consistent posture because getting it wrong is a trust and compliance failure, not just a delayed message; and channel/provider redundancy that keeps a single vendor's outage from becoming a full communication outage.

Nearly every architectural pattern covered elsewhere in this series shows up here in service of that bar — DDD's aggregates enforcing an auditable, explicit suppression path rather than a silent drop, Event-Driven Architecture's idempotent fan-out, Resilience's provider fallback chains, and the full observability trio watching over delivery health per channel and per provider. A notification system is, in that sense, less a distinct discipline from everything else in this series than the place where its cumulative lessons about idempotency, honest state representation, and respecting explicit user consent matter more constantly, and more unforgivingly, than almost anywhere else — precisely because it's the one system nearly every other system in the architecture eventually calls.

---

*Found this useful? Feel free to star the repo, open an issue with corrections, or share the "sent one more email after the unsubscribe" incident that turned out to matter far more than a delivery-latency number ever should.*
