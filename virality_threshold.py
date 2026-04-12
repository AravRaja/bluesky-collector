"""User-owned virality definition.

Edit the is_viral() function below to define your own threshold.
export.py imports and calls this for each root post to decide labeling.
"""


def is_viral(likes, reposts, replies, quotes, follower_count):
    """
    Define your virality threshold here.
    Called by export.py for each root post to decide if it's viral.

    Args:
        likes:          total likes on the post
        reposts:        total reposts
        replies:        total direct replies
        quotes:         total quote-posts
        follower_count: post author's follower count (from profiles table)

    Returns:
        True if the post should be labelled viral, False otherwise.
    """
    return reposts >= 100
