def flatten_description(description):
    """Flatten Atlassian Document Format (ADF) description to plain text."""
    if not description or not description.get('content'):
        return ''
    
    return process_content_array(description['content']).strip()


def process_content_array(content):
    """Process an array of content nodes."""
    if not content:
        return ''
    return ''.join(process_node(node) for node in content)


def process_node(node):
    """Process a single content node based on its type."""
    if not node or not isinstance(node, dict):
        return ''
    
    node_type = node.get('type', '')
    
    if node_type == 'paragraph':
        return process_paragraph(node) + '\n\n'
    
    elif node_type == 'heading':
        return process_heading(node) + '\n\n'
    
    elif node_type == 'bulletList':
        return process_bullet_list(node) + '\n\n'
    
    elif node_type == 'listItem':
        return process_list_item(node)
    
    elif node_type == 'codeBlock':
        return process_code_block(node) + '\n\n'
    
    elif node_type == 'text':
        return process_text(node)
    
    elif node_type == 'inlineCard':
        return process_inline_card(node)
    
    elif node_type == 'media':
        return process_media(node)
    
    elif node_type == 'mediaSingle':
        return process_media_single(node) + '\n\n'
    
    elif node_type == 'rule':
        return '---\n\n'
    
    elif node_type == 'hardBreak':
        return '\n'
    
    elif node_type == 'mention':
        return process_mention(node)
    
    else:
        # Handle other node types by processing their content if available
        if node.get('content'):
            return process_content_array(node['content'])
        return ''


def process_paragraph(node):
    """Process a paragraph node."""
    content = node.get('content')
    if not content:
        return ''
    return process_content_array(content)


def process_heading(node):
    """Process a heading node."""
    attrs = node.get('attrs', {})
    level = attrs.get('level', 1)
    heading_prefix = '#' * level
    
    content = node.get('content')
    if not content:
        return f'{heading_prefix} '
    
    heading_text = process_content_array(content)
    return f'{heading_prefix} {heading_text}'


def process_bullet_list(node):
    """Process a bullet list node."""
    content = node.get('content')
    if not content:
        return ''
    return process_content_array(content)


def process_list_item(node):
    """Process a list item node."""
    content = node.get('content')
    if not content:
        return ''
    
    item_content = process_content_array(content)
    # Remove extra newlines and format as list item
    clean_content = item_content.rstrip('\n')
    return f'* {clean_content}\n'


def process_code_block(node):
    """Process a code block node."""
    content = node.get('content')
    if not content:
        return '```\n\n```'
    
    code_content = process_content_array(content)
    language = code_content.strip()
    
    # If the content is just a language identifier, return empty code block with language
    if language and len(language) < 20 and '\n' not in language:
        return f'```{language}\n\n```'
    
    return f'```\n{code_content}```'


def process_text(node):
    """Process a text node with optional formatting marks."""
    text = node.get('text', '')
    if not text:
        return ''
    
    marks = node.get('marks', [])
    
    # Check if this text has code marking
    if any(mark.get('type') == 'code' for mark in marks):
        text = f'```{text}```'
    
    # Apply other marks (bold, italic, etc.)
    for mark in marks:
        mark_type = mark.get('type', '')
        if mark_type == 'strong':
            text = f'**{text}**'
        elif mark_type == 'em':
            text = f'*{text}*'
        # Add other mark types as needed
    
    return text


def process_inline_card(node):
    """Process an inline card node."""
    attrs = node.get('attrs', {})
    url = attrs.get('url')
    if url:
        return url
    return ''


def process_media(node):
    """Process a media node."""
    attrs = node.get('attrs', {})
    alt = attrs.get('alt')
    if alt:
        return f'[Image: {alt}]'
    return '[Image]'


def process_media_single(node):
    """Process a media single node."""
    content = node.get('content')
    if not content:
        return ''
    return process_content_array(content)


def process_mention(node):
    """Process a mention node."""
    attrs = node.get('attrs', {})
    text = attrs.get('text')
    if text:
        return text
    return ''


# Example usage:
if __name__ == '__main__':
    import json

    # Load the JSON file
    with open('issue.json', 'r') as f:
        data = json.load(f)

    # Extract and print the description
    description = data["record"]["fields"]["description"]
    
    result = flatten_description(description)
    print(result)
