export type DatabaseSchema = {
  post: Post
  sub_state: SubState
  graph_follow: GraphFollow
  graph_interaction: GraphInteraction
  graph_meta: GraphMeta
  user_influential_l2: UserInfluentialL2
  user_served_post: UserServedPost
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
