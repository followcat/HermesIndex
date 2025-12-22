-- Recommended indexes for faster sync on bitmagnet.

-- torrents
CREATE INDEX IF NOT EXISTS idx_torrents_updated_at ON public.torrents(updated_at);

-- torrent_files
CREATE INDEX IF NOT EXISTS idx_torrent_files_updated_at ON public.torrent_files(updated_at);

-- content
CREATE INDEX IF NOT EXISTS idx_content_updated_at ON public.content(updated_at);

-- joins
CREATE INDEX IF NOT EXISTS idx_torrent_tags_info_hash ON public.torrent_tags(info_hash);
CREATE INDEX IF NOT EXISTS idx_torrent_hints_info_hash ON public.torrent_hints(info_hash);
CREATE INDEX IF NOT EXISTS idx_content_attributes_keys ON public.content_attributes(content_type, content_source, content_id, source, key);
CREATE INDEX IF NOT EXISTS idx_content_collections_content_key ON public.content_collections_content(content_type, content_source, content_id);
