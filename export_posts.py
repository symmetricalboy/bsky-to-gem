import json
import os
import sys
from datetime import datetime

import requests
from atproto import Client


def fetch_did_document(did: str, timeout: int = 10):

def count_tokens_with_google_tokenizer(text):
    """
    Count tokens using Google's FLAN-T5 tokenizer for accurate Gemini estimates.
    Returns token count or None if tokenizer unavailable.
    """
    try:
        from transformers import AutoTokenizer
        print("🔢 Counting tokens using Google FLAN-T5 tokenizer...")
        tokenizer = AutoTokenizer.from_pretrained("google/flan-t5-small")
        tokens = tokenizer.encode(text, add_special_tokens=True)
        return len(tokens)
    except ImportError:
        print("⚠️  transformers not installed. Run: pip install transformers")
        return None
    except Exception as e:
        print(f"⚠️  Error counting tokens: {e}")
        return None

def check_token_limit_and_offer_trim(filename, all_posts, handle):
    """
    Check if the exported JSON exceeds token limits and offer to trim if needed.
    """
    TOKEN_LIMIT = 950000  # 95% of 1M tokens
    
    # Read the exported file
    with open(filename, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Count tokens
    token_count = count_tokens_with_google_tokenizer(content)
    
    if token_count is None:
        print("\n⚠️  Could not count tokens. Install transformers for token analysis.")
        return filename
    
    print(f"\n📊 Token Analysis:")
    print(f"   Total tokens: {token_count:,}")
    print(f"   Limit (95% of 1M): {TOKEN_LIMIT:,}")
    
    if token_count <= TOKEN_LIMIT:
        print(f"✅ Token count is within limits! Ready for Gemini prompting.")
        return filename
    
    # Calculate how many posts to remove
    excess_tokens = token_count - TOKEN_LIMIT
    avg_tokens_per_post = token_count // len(all_posts)
    posts_to_remove = int(excess_tokens / avg_tokens_per_post * 1.1)  # 10% buffer
    posts_to_keep = len(all_posts) - posts_to_remove
    
    print(f"\n⚠️  TOKEN LIMIT EXCEEDED!")
    print(f"   Excess tokens: {excess_tokens:,}")
    print(f"   Estimated posts to remove: {posts_to_remove:,} (oldest)")
    print(f"   Posts that would remain: {posts_to_keep:,}")
    print(f"\n💡 This dataset is too large for effective Gemini prompting.")
    
    # Offer to trim
    while True:
        choice = input("\n🤔 Remove oldest posts automatically to fit limit? (y/n): ").strip().lower()
        if choice in ['y', 'yes']:
            return trim_posts_and_reexport(filename, all_posts, posts_to_keep, handle)
        elif choice in ['n', 'no']:
            print("\n📁 Keeping full export. You may need to manually trim for Gemini use.")
            return filename
        else:
            print("Please enter 'y' or 'n'")

def trim_posts_and_reexport(original_filename, all_posts, posts_to_keep, handle):
    """
    Create a new trimmed export with only the newest posts.
    """
    print(f"\n✂️  Trimming to newest {posts_to_keep:,} posts...")
    
    # Keep only the newest posts (already sorted newest first)
    trimmed_posts = all_posts[:posts_to_keep]
    
    # Create new filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    trimmed_filename = f"{handle}_posts_{timestamp}_trimmed.json"
    
    # Write trimmed data
    with open(trimmed_filename, 'w', encoding='utf-8') as f:
        json.dump(trimmed_posts, f, indent=2, ensure_ascii=False)
    
    # Verify token count of trimmed file
    with open(trimmed_filename, 'r', encoding='utf-8') as f:
        trimmed_content = f.read()
    
    trimmed_tokens = count_tokens_with_google_tokenizer(trimmed_content)
    
    print(f"\n✅ Trimmed export created!")
    print(f"📁 Original: {original_filename} ({len(all_posts):,} posts)")
    print(f"📁 Trimmed: {trimmed_filename} ({len(trimmed_posts):,} posts)")
    
    if trimmed_tokens:
        print(f"🔢 Trimmed tokens: {trimmed_tokens:,}")
        if trimmed_tokens <= 950000:
            print(f"✅ Trimmed file is within token limits!")
        else:
            print(f"⚠️  Trimmed file may still be too large. Consider further trimming.")
    
    return trimmed_filename

def export_posts_to_json(handle):

    """
    Fetch the DID document for a did:plc or did:web DID.

    Returns the parsed JSON DID document, or None if it couldn't be fetched.
    """
    try:
        if did.startswith("did:plc:"):
            url = f"https://plc.directory/{did}"
            r = requests.get(url, timeout=timeout)
            r.raise_for_status()
            return r.json()
        elif did.startswith("did:web:"):
            # did:web:example.com -> domain = example.com OR did:web:sub:example.com -> join rest with ':'
            domain = did.split(":", 2)[2]
            # did:web encoding uses colons for path segments; convert to normal domain/path
            domain = domain.replace(":", "/")
            url = f"https://{domain}/.well-known/did.json"
            r = requests.get(url, timeout=timeout)
            r.raise_for_status()
            return r.json()
        else:
            return None
    except requests.exceptions.RequestException:
        return None

def get_pds_endpoint_from_did_doc(did_doc: dict):
    """
    Parse a DID document and return the serviceEndpoint for the first service
    entry whose id endswith '#atproto_pds' and whose type indicates
    AtprotoPersonalDataServer.
    """
    if not did_doc:
        return None
    services = did_doc.get("service") or did_doc.get("services") or []
    for svc in services:
        sid = str(svc.get("id", ""))
        stype = svc.get("type", "")
        if sid.endswith("#atproto_pds") or stype == "AtprotoPersonalDataServer":
            endpoint = svc.get("serviceEndpoint")
            if isinstance(endpoint, str) and endpoint:
                return endpoint.rstrip("/")  # ensure no trailing slash
    return None

def export_posts_to_json(handle):
    """
    Fetches all posts from an atproto account and saves them to a timestamped JSON file.
    """
    # First, use the default client to resolve the handle to a DID
    default_client = Client()  # uses default/public host for resolution
    print(f"🔍 Resolving handle: {handle}")

    try:
        identity_response = default_client.com.atproto.identity.resolve_handle({'handle': handle})
        repo_did = identity_response.did
        print(f"✅ Found DID: {repo_did}")
    except Exception as e:
        print(f"❌ Error resolving handle '{handle}': {e}")
        print("💡 Make sure the handle is correct (e.g., user.bsky.social)")
        sys.exit(1)

    # Try to fetch the DID document (PLC or web) and parse the PDS endpoint.
    pds_endpoint = None
    did_doc = fetch_did_document(repo_did)
    if did_doc:
        pds_endpoint = get_pds_endpoint_from_did_doc(did_doc)
        if pds_endpoint:
            print(f"🔗 Resolved PDS endpoint from DID document: {pds_endpoint}")
        else:
            print("⚠️ DID document found but no #atproto_pds entry present.")
    else:
        print("⚠️ Could not fetch DID document from PLC or did:web well-known; will fall back to public resolver.")

    # Use a client targeted at the account's PDS if we found one, otherwise use default client.
    if pds_endpoint:
        client = Client(base_url=pds_endpoint)
    else:
        client = default_client

    # Create timestamped filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"{handle}_posts_{timestamp}.json"
    
    all_posts = []
    cursor = None
    posts_fetched = 0
    image_cdn_base = "https://cdn.bsky.app/img/feed_fullsize/plain"

    print("Starting to fetch posts... This may take a while if you have many posts.")

    while True:
        try:
            response = client.com.atproto.repo.list_records(
                {
                    'repo': repo_did,
                    'collection': 'app.bsky.feed.post',
                    'limit': 100,
                    'cursor': cursor,
                }
            )

            # If response.records is empty/falsey
            if not getattr(response, "records", None):
                if posts_fetched == 0:
                    # No records found on this PDS. If we were using a fallback public client,
                    # it may simply be pointing at the wrong PDS. Surface an actionable error.
                    if pds_endpoint:
                        print("No more posts found.")
                        break
                    else:
                        print("❌ No posts found at the public resolver. The account may be hosted on a different PDS.")
                        print("💡 If this is a custom domain or migrated account, its PDS endpoint must be discovered from the DID document.")
                        sys.exit(1)

            for record in response.records:
                post_data = {
                    'created_at': record.value.created_at,
                    'text': record.value.text,
                    'images': []
                }

                if hasattr(record.value, 'embed') and record.value.embed:
                    if getattr(record.value.embed, "py_type", "") == 'app.bsky.embed.images':
                        for image in record.value.embed.images:
                            image_url = f"{image_cdn_base}/{repo_did}/{image.image.cid}@jpeg"
                            post_data['images'].append({
                                'url': image_url,
                                'alt_text': image.alt
                            })

                all_posts.append(post_data)

            posts_fetched += len(response.records)
            print(f"Fetched {posts_fetched} posts so far...")
            
            cursor = getattr(response, "cursor", None)
            if not cursor:
                print("Reached end of data.")
                break

        except Exception as e:
            print(f"❌ Error fetching posts: {e}")
            # If we were using a specific PDS client and it failed, try falling back to the public client once.
            if pds_endpoint and client is not default_client:
                print("ℹ️ Attempting fallback: switching to public resolver client and retrying once...")
                client = default_client
                pds_endpoint = None  # mark that we are no longer using custom PDS
                continue
            sys.exit(1)

    if not all_posts:
        print("❌ Export failed: no posts to save.")
        sys.exit(1)

    all_posts.sort(key=lambda x: x['created_at'], reverse=True)

    with open(output_filename, 'w', encoding='utf-8') as f:
        json.dump(all_posts, f, indent=2, ensure_ascii=False)

    print(f"\n✅ Export complete!")
    print(f"📊 Total posts exported: {len(all_posts)}")
    print(f"💾 Export saved to: {output_filename}")
    
    # Check token limits and offer trimming if needed
    final_filename = check_token_limit_and_offer_trim(output_filename, all_posts, handle)
    
    if final_filename != output_filename:
        print(f"\n🎯 Use this file for Gemini prompting: {final_filename}")
    else:
        print(f"\n🎯 File ready for Gemini prompting: {final_filename}")
    
    return final_filename


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("🦋 Bluesky Posts Export Tool")
        print("=" * 40)
        print("Usage: python export_posts.py <handle>")
        sys.exit(1)
    
    handle = sys.argv[1]
    print(f"🎯 Target account: {handle}")
    print(f"📥 Starting full export (no authentication)...")

    export_posts_to_json(handle)

        
    final_file = export_posts_to_json(handle)
    
    if final_file:
        print(f"\n🚀 Ready to use with Gemini!")
        print(f"📋 Tip: Copy the contents of '{final_file}' along with your prompt template.")
