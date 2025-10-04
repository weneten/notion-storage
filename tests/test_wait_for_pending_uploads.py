import json
import subprocess
from pathlib import Path


def test_wait_for_pending_uploads(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    streaming_js = project_root / "static" / "streaming-upload.js"

    node_script = f"""
const fs = require('fs');
const vm = require('vm');

const code = fs.readFileSync('{streaming_js.as_posix()}', 'utf8');

const messageLog = [];
const messageContainer = {{
    innerHTML: '',
    appendChild(node) {{
        node.parentNode = this;
        messageLog.push(node.innerHTML);
    }},
    removeChild(node) {{
        const idx = messageLog.indexOf(node.innerHTML);
        if (idx >= 0) {{
            messageLog.splice(idx, 1);
        }}
    }},
}};

const progressContainer = {{
    innerHTML: '<div>progress</div>',
}};

const createStubElement = () => {{
    return {{
        style: {{}},
        textContent: '',
        innerHTML: '',
        appendChild() {{}},
        removeChild() {{}},
    }};
}};

const context = {{
    console: console,
    setTimeout: setTimeout,
    clearTimeout: clearTimeout,
}};

context.window = context;
context.window.pendingUploads = new Set();
context.window.currentFolder = '/';
context.window.location = {{ origin: 'http://localhost' }};
context.document = {{
    getElementById(id) {{
        if (id === 'messageContainer') {{
            return messageContainer;
        }}
        if (id === 'progressBars') {{
            return progressContainer;
        }}
        return createStubElement();
    }},
    createElement(tag) {{
        return {{
            tagName: tag,
            className: '',
            innerHTML: '',
            style: {{}},
            appendChild(child) {{
                this.child = child;
            }},
        }};
    }},
    querySelectorAll() {{
        return [];
    }},
    addEventListener() {{
        return undefined;
    }},
}};

vm.createContext(context);
vm.runInContext(code, context);

const results = {{
    messageLog: messageLog,
    progressBefore: progressContainer.innerHTML,
    progressAfter: null,
    pendingCount: null,
    fetchCount: 0,
    error: null,
}};

context.window.fetch = async (url) => {{
    results.fetchCount += 1;
    return {{
        ok: true,
        json: async () => ({{ status: 'success' }}),
    }};
}};

(async () => {{
    context.window.pendingUploads.add('upload-1');
    try {{
        await context.window.waitForPendingUploads();
        progressContainer.innerHTML = '';
        results.pendingCount = context.window.pendingUploads.size;
    }} catch (err) {{
        results.error = err.message;
    }}
    results.progressAfter = progressContainer.innerHTML;
    console.log(JSON.stringify(results));
}})().catch((err) => {{
    results.error = err.message;
    results.progressAfter = progressContainer.innerHTML;
    console.log(JSON.stringify(results));
}});
"""

    completed = subprocess.run(
        ["node", "-e", node_script],
        check=True,
        capture_output=True,
        text=True,
    )

    output_line = completed.stdout.strip().splitlines()[-1]
    data = json.loads(output_line)

    assert data["error"] is None
    assert data["fetchCount"] >= 1
    assert data["pendingCount"] == 0
    assert data["progressBefore"] != ''
    assert data["progressAfter"] == ''
    assert any(
        'Waiting for server to finalize' in message
        for message in data["messageLog"]
    ), data["messageLog"]
