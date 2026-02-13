export type DatabaseSchema = {
  post: Post
  sub_state: SubState
  graph_follow: GraphFollow
  graph_interaction: GraphInteraction
  graph_meta: GraphMeta
  user_influential_l2: UserInfluentialL2
  user_served_post: UserServedPost
  user_seen_post: UserSeenPost
  user_keyword: UserKeyword
  taste_similarity: TasteSimilarity
  taste_reputation: TasteReputation
  user_author_fatigue: UserAuthorFatigue
  feed_debug_log: FeedDebugLog
}

export type Post = {
  uri: string
  cid: string
  indexedAt: string
  author: string
  likeCount: number
  replyCount: number
  repostCount: number
  replyRoot: string | null
  replyParent: string | null
  text: string | null
  hasImage: number
  hasVideo: number
  hasExternal: number
}

export type SubState = {
  service: string
  cursor: number
}

export type GraphFollow = {
  follower: string
  followee: string
  indexedAt: string
}

export type GraphInteraction = {
  actor: string
  target: string
  type: 'like' | 'repost' | 'reply'
  weight: number
  indexedAt: string
}

export type GraphMeta = {
  key: string
  value: string
  updatedAt: string
}

export type UserInfluentialL2 = {
  userDid: string
  l2Did: string
  influenceScore: number
  l1FollowerCount: number
  updatedAt: string
}

export type UserServedPost = {
  userDid: string
  uri: string
  servedAt: string
}

export type UserSeenPost = {
  userDid: string
  uri: string
  seenAt: string
}

export type UserKeyword = {
  userDid: string
  keyword: string
  score: number
  updatedAt: string
}

export type TasteSimilarity = {
  userDid: string
  similarUserDid: string
  agreementCount: number
  totalCoLikedPosts: number
  lastAgreementAt: string
  updatedAt: string
}

export type TasteReputation = {
  userDid: string
  similarUserDid: string
  reputationScore: number
  agreementHistory: number // positive for agreements, negative for disagreements
  lastSeenAt: string
  decayRate: number
  updatedAt: string
}

export type UserAuthorFatigue = {
  userDid: string
  authorDid: string
  serveCount: number
  lastServedAt: string
  fatigueScore: number // 0-100, higher means more fatigued
  affinityScore: number // 0-10, warmer connection
  interactionWeight: number // frequency/quality of interactions
  lastInteractionAt: string | null
  interactionCount: number
  updatedAt: string
}

export type FeedDebugLog = {
  userDid: string
  uri: string
  score: number
  signals: string // JSON string containing the breakdown
  servedAt: string
}
