import { AppContext } from '../config'
import {
  QueryParams,
  OutputSchema as AlgoOutput,
} from '../lexicon/types/app/bsky/feed/getFeedSkeleton'
import * as whatsAlf from './whats-alf'
import * as socialGraph from './social-graph'

type AlgoHandler = (
  ctx: AppContext,
  params: QueryParams,
  requesterDid: string,
) => Promise<AlgoOutput>

const algos: Record<string, AlgoHandler> = {
  [whatsAlf.shortname]: socialGraph.handler,
  [socialGraph.shortname]: socialGraph.handler,
}

export default algos
